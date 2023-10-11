using JSC = NiL.JS.Core;
using JSL = NiL.JS.BaseLibrary;
using LiteDB;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Security.Cryptography.X509Certificates;
using System.Security.Policy;
using System.IO;
using System.Threading;

namespace X13 {
  internal class LiteDB : IDisposable {
    private const string DB_PATH = "../data/persist.ldb";
    private const string DBA_PATH = "../data/archive.ldb";

    private static string EscapFieldName(string fn) {
      if(string.IsNullOrEmpty(fn)) {
        throw new ArgumentNullException("LiteDB.EscapFieldName()");
      }
      StringBuilder sb = new StringBuilder();

      for(var i = 0; i < fn.Length; i++) {
        var c = fn[i];

        if(char.IsLetterOrDigit(c) || (c == '$' && i == 0) || (c == '-' && i > 0)) {
          sb.Append(c);
        } else {
          sb.Append("_");
          sb.Append(((ushort)c).ToString("X4"));
        }
      }
      return sb.ToString();
    }
    private static string UnescapFieldName(string fn) {
      if(string.IsNullOrEmpty(fn)) {
        throw new ArgumentNullException("LiteDB.UnescapFieldName()");
      }
      StringBuilder sb = new StringBuilder();
      for(var i = 0; i < fn.Length; i++) {
        var c = fn[i];
        if(c == '_' && i + 4 < fn.Length && ushort.TryParse(fn.Substring(i + 1, 4), System.Globalization.NumberStyles.HexNumber, System.Globalization.CultureInfo.InvariantCulture, out ushort cc)) {
          i += 4;
          sb.Append((char)cc);
        } else {
          sb.Append(c);
        }
      }
      return sb.ToString();
    }
    private BsonValue Js2Bs(JSC.JSValue val) {
      if(val == null) {
        return BsonValue.Null;
      }
      switch(val.ValueType) {
        case JSC.JSValueType.NotExists:
        case JSC.JSValueType.NotExistsInObject:
        case JSC.JSValueType.Undefined:
          return BsonValue.Null;
        case JSC.JSValueType.Boolean:
          return new BsonValue((bool)val);
        case JSC.JSValueType.Date: {
            if(val.Value is JSL.Date jsd) {
              return new BsonValue(jsd.ToDateTime().ToUniversalTime());
            }
            return BsonValue.Null;
          }
        case JSC.JSValueType.Double:
          return new BsonValue((double)val);
        case JSC.JSValueType.Integer:
          return new BsonValue((int)val);
        case JSC.JSValueType.String: {
            var s = val.Value as string;
            //if(s != null && s.StartsWith("¤TR")) {
            //  var t = Topic.I.Get(Topic.root, s.Substring(3), false, null, false, false);
            //  if(t != null) {
            //    if(_base.TryGetValue(t, out Stash tu)) {
            //      return tu.bm["_id"];
            //    }
            //  }
            //  throw new ArgumentException("TopicRefernce(" + s.Substring(3) + ") NOT FOUND");
            //}
            return new BsonValue(s);
          }
        case JSC.JSValueType.Object:
          if(val.IsNull) {
            return BsonValue.Null;
          }
          if(val is JSL.Array arr) {
            var r = new BsonArray();
            foreach(var f in arr) {
              if(int.TryParse(f.Key, out int i)) {
                while(i >= r.Count()) { r.Add(BsonValue.Null); }
                r[i] = Js2Bs(f.Value);
              }
            }
            return r;
          }
          ByteArray ba = val as ByteArray;
          if(ba != null || (ba = val.Value as ByteArray) != null) {
            return new BsonValue(ba.GetBytes());
          } {
            var r = new BsonDocument();
            foreach(var f in val) {
              r[EscapFieldName(f.Key)] = Js2Bs(f.Value);
            }
            return r;
          }
        default:
          throw new NotImplementedException("js2Bs(" + val.ValueType.ToString() + ")");
      }
    }
    private string Id2Topic(ObjectId id) {
      var d = _objects.FindById(id);
      BsonValue p;
      if(d != null && (p = d["p"]) != null && p.IsString) {
        return p.AsString;
      }
      return null;
    }
    private JSC.JSValue Bs2Js(BsonValue val) {
      if(val == null) {
        return JSC.JSValue.Undefined;
      }
      switch(val.Type) { //-V3002
        case BsonType.ObjectId: {
            var p = Id2Topic(val.AsObjectId);
            if(p != null) {
              return new JSL.String("¤TR" + p);
            } else {
              throw new ArgumentException("Unknown ObjectId: " + val.AsObjectId.ToString());
            }
          }
        case BsonType.Array: {
            var arr = val.AsArray;
            var r = new JSL.Array(arr.Count);
            for(int i = 0; i < arr.Count; i++) {
              if(!arr[i].IsNull) {
                r[i] = Bs2Js(arr[i]);
              }
            }
            return r;
          }
        case BsonType.Boolean:
          return new JSL.Boolean(val.AsBoolean);
        case BsonType.DateTime:
          return JSC.JSValue.Marshal(val.AsDateTime.ToLocalTime());
        case BsonType.Binary:
          return new ByteArray(val.AsBinary);
        case BsonType.Document: {
            var r = JSC.JSObject.CreateObject();
            var o = val.AsDocument;
            foreach(var i in o) {
              r[UnescapFieldName(i.Key)] = Bs2Js(i.Value);
            }
            return r;
          }
        case BsonType.Double: {
            return new JSL.Number(val.AsDouble);
          }
        case BsonType.Int32:
          return new JSL.Number(val.AsInt32);
        case BsonType.Int64:
          return new JSL.Number(val.AsInt64);
        case BsonType.Null:
          return JSC.JSValue.Null;
        case BsonType.String:
          return new JSL.String(val.AsString);
      }
      throw new NotImplementedException("Bs2Js(" + val.Type.ToString() + ")");
    }

    private LiteDatabase _db;
    private ILiteCollection<BsonDocument> _objects, _states, _history;

    private LiteDatabase _dba;
    private ILiteCollection<BsonDocument> _archive;

    public LiteDB() {

    }
    public void Open() {
      if(!File.Exists(DB_PATH)) {
        Log.Error("LiteDB.Open({0}) - not exist", Path.GetFullPath(DB_PATH));
        throw new TaskCanceledException();
      }
      _db = new LiteDatabase(new ConnectionString { Upgrade = true, Filename = DB_PATH }) { CheckpointSize = 50 };
      if(!_db.CollectionExists("objects")) {
        Log.Error("LiteDB.Open() - Collection objects not exist");
        throw new TaskCanceledException();
      }
      if(!_db.CollectionExists("states")) {
        Log.Error("LiteDB.Open() - Collection states not exist");
        throw new TaskCanceledException();
      }
      _objects = _db.GetCollection<BsonDocument>("objects");
      _states = _db.GetCollection<BsonDocument>("states");


      if(_db.CollectionExists("history")) {
        _history = _db.GetCollection<BsonDocument>("history");
      }
      if(!File.Exists(DBA_PATH)) {
        Log.Error("LiteDB.Open({0}) - not exist", Path.GetFullPath(DBA_PATH));
        throw new TaskCanceledException();
      }
      _dba = new LiteDatabase(new ConnectionString { Upgrade = true, Filename = DBA_PATH }) { CheckpointSize = 100 };
      _archive = _dba.GetCollection<BsonDocument>("archive");
    }

    public IEnumerable<Topic> Topics() {
      return _objects.FindAll().OrderBy(z => z["p"])
        .Select(z => {
          var bs = _states.FindById(z["_id"]);
          return new Topic { path = z["p"].AsString, manifest = Bs2Js(z["v"]), state = bs==null?JSC.JSValue.Undefined:Bs2Js(bs["v"]) };
        });
    }
    public IEnumerable<Log.LogRecord> LogRecords() {
      if(_history == null) {
        return new Log.LogRecord[0];
      }
      return _history.Query().Where("$.t > @0", DateTime.Now.AddDays(-14)) .OrderBy("t").ToEnumerable()
        .Select(z => new Log.LogRecord {
          dt = z["t"].AsDateTime.ToLocalTime(),
          ll = (LogLevel)z["l"].AsInt32,
          format = z["m"].AsString,
          args = null
        });
    }

    public IEnumerable<ArchRecord> ArchRecords() {
      return _archive.Query().OrderBy("t").ToEnumerable()
        .Select(z => new ArchRecord { dt = z["t"].AsDateTime.ToLocalTime(), path = z["p"].AsString, value = z["v"].AsDouble });
    }

    public void Dispose() {
      var dba = Interlocked.Exchange(ref _dba, null);
      if(dba != null) {
        try {
          dba.Commit();
          dba.Checkpoint();
        }
        catch(Exception ex) {
          Log.Warning("LiteDB.Dispose.Arch - {0}", ex);
        }
        dba.Dispose();
      }
      var db = Interlocked.Exchange(ref _db, null);
      if(db != null) {
        try {
          db.Commit();
          db.Checkpoint();
        }
        catch(Exception ex) {
          Log.Warning("LiteDB.Dispose.DB - {0}", ex);
        }
        db.Dispose();
      }
    }
  }
}
