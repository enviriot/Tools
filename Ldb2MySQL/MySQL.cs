using JSC = NiL.JS.Core;
using JSL = NiL.JS.BaseLibrary;
using MySqlConnector;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Security.Cryptography;
using NiL.JS.Extensions;

namespace X13 {
  internal class MySQL : IDisposable {
    private const string DB_NAME = "Enviriot";
    private MySqlConnectionStringBuilder _builder;
    private MySqlConnection _db;
    private readonly Dictionary<string, long> _idCache;

    public MySQL(Uri uri) {
      _idCache = new Dictionary<string, long>();
      _builder = new MySqlConnectionStringBuilder { Server = uri.DnsSafeHost };
      if(!string.IsNullOrEmpty(uri.UserInfo)) {
        var items = uri.UserInfo.Split(new[] { ':' });
        if(items.Length > 1) {
          _builder.UserID = items[0];
          _builder.Password = items[1];
        } else {
          _builder.UserID = uri.UserInfo;
        }
      }
      if(uri.Port > 0) {
        _builder.Port = (uint)uri.Port;
      }
    }
    public void Create() {
      var db = new MySqlConnection(_builder.ConnectionString);
      try {
        db.Open();
      }
      catch(MySqlException ex) {
        Log.Error("MySQL.Create.OpenDB - {0}", ex.Message);
        throw new TaskCanceledException();
      }
      bool exist;
      using(var cmd = db.CreateCommand()) {
        cmd.CommandText = "select count(*) from INFORMATION_SCHEMA.SCHEMATA where SCHEMA_NAME = @name;";
        cmd.Parameters.AddWithValue("name", DB_NAME);
        exist = 1 == (long)cmd.ExecuteScalar();
      }
      if(exist) {
        Log.Warning("{0} DB already exist", DB_NAME);
        throw new TaskCanceledException();
      }
      using(var cmd = db.CreateCommand()) {
        cmd.CommandText = "create database " + DB_NAME + ";";
        cmd.ExecuteNonQuery();
      }
      db.Close();
      _builder.Database = DB_NAME;
      _db = new MySqlConnection(_builder.ConnectionString);
      _db.Open();
      using(var batch = _db.CreateBatch()) {
        batch.BatchCommands.Add(new MySqlBatchCommand("create table PS(ID int auto_increment primary key, P text not null, M text, S text);"));
        batch.BatchCommands.Add(new MySqlBatchCommand("create table LOGS(ID int auto_increment primary key, DT datetime(3) not null, L tinyint not null, M text not null, key `LOGS_DT_IDX` (`DT`) using btree);"));
        batch.BatchCommands.Add(new MySqlBatchCommand("create table ARCH(ID int not null auto_increment, P int not null, DT datetime(3) not null, V double not null, primary key (ID), key ARCH_FK (p), key ARCH_DT_IDX (DT) using btree, constraint ARCH_FK foreign key (P) references PS(ID) on delete cascade)"));
        batch.BatchCommands.Add(new MySqlBatchCommand("create table ARCH_W(P int primary key, DT1 datetime(3), constraint ARCH_W_FK foreign key (P) references PS(ID) on delete cascade);"));
        batch.BatchCommands.Add(new MySqlBatchCommand("create trigger ARCH_DATA after update on PS for each row begin if (new.S != old.S and json_value(new.M, '$.Arch.enable') and (json_type(new.S) = 'INTEGER' or json_type(new.S) = 'DOUBLE')) then insert into ARCH (P, DT, V) values (new.ID, now(3), json_value(new.S, '$')); insert ignore into ARCH_W(P) values(new.ID); end if; end"));
        batch.BatchCommands.Add(new MySqlBatchCommand("create procedure PurgeArchProc() begin declare v_id int(11); declare v_keep double; select aw.P, ifnull(cast(json_value(ps.M, '$.Arch.keep') as double), 7)*60*60*24 as KEEP into v_id, v_keep from ARCH_W aw join PS ps on aw.P = ps.ID where DT1 < now() or DT1 is null limit 1; if v_id is not null then delete from ARCH where p = v_id AND DT < timestampadd(second, -v_keep, now(3)); update ARCH_W set dt1 = timestampadd(second, least(2*v_keep, 60*60*(12+12*rand())), now(3)) where p = v_id; end if; end"));
        batch.BatchCommands.Add(new MySqlBatchCommand("set global event_scheduler = on;"));
        batch.BatchCommands.Add(new MySqlBatchCommand("create event PurgeLogs on schedule every 1 day starts current_date + interval 1 day + interval 3 hour + interval 45 minute do delete from LOGS where DT + interval 60 day < now();"));
        batch.BatchCommands.Add(new MySqlBatchCommand("create event PurgeArch on schedule every 10 second do call PurgeArchProc();"));
        batch.ExecuteNonQuery();
      }
    }
    public void Write(Topic t) {
      using(MySqlCommand cmd = _db.CreateCommand()) {
        if(t.state.IsUndefined()) {
          cmd.CommandText = "insert into PS (P, M) values (@path, @manifest);";
        } else {
          cmd.CommandText = "insert into PS (P, M, S) values (@path, @manifest, @state);";
          cmd.Parameters.AddWithValue("state", JsLib.Stringify(t.state));
        }
        cmd.Parameters.AddWithValue("path", t.path);
        cmd.Parameters.AddWithValue("manifest", JsLib.Stringify(t.manifest));
        cmd.ExecuteNonQuery();
        _idCache[t.path] = cmd.LastInsertedId;
      }
    }
    public void Write(Log.LogRecord l) {
      ExecuteNonQuery("insert into LOGS(DT, L, M) values(@P0, @P1, @P2);", l.dt, (int)l.ll, l.format);
    }

    public void Write(ArchRecord r) {
      if(double.IsNaN(r.value)) {
        return;
      }
      if(!_idCache.TryGetValue(r.path, out long  id)) {
        Log.Debug("WriteArch({0}) no Id", r.path);
        return;
      }
      ExecuteNonQuery("insert into ARCH (P, DT, V) values (@P0, @P1, @P2);", id, r.dt, r.value);
    }

    private void ExecuteNonQuery(string command, params object[] args) {
      try {
        using(MySqlCommand cmd = new MySqlCommand(command, _db)) {
          for(int i = 0; i < args.Length; i++) {
            cmd.Parameters.AddWithValue("P" + i.ToString(), args[i]);
          }
          cmd.ExecuteNonQuery();
        }
      }
      catch(Exception ex) {
        Log.Error("MySQL.ExecuteNonQuery({0}) - {1}", command, ex.Message);
        throw new TaskCanceledException();
      }
    }

    public void Dispose() {
      var db = Interlocked.Exchange(ref _db, null);
      if(db != null) {
        try {
          db.Close();
        }
        catch(Exception ex) {
          Log.Warning("MySQL.Dispose - {0}", ex);
        }
      }
    }
  }
}
