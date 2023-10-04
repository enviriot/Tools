using JSC = NiL.JS.Core;
using JSF = NiL.JS.Core.Functions;
using JSI = NiL.JS.Core.Interop;
using JSL = NiL.JS.BaseLibrary;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace X13 {
  internal static class JsLib {
    public static readonly char[] SPLITTER_OBJ = new char[] { '.' };
    private static JSF.ExternalFunction _SJ_Replacer;
    static JsLib() {
      _SJ_Replacer = new JSF.ExternalFunction(SJ_CustomTypesRepl);
    }
    private static JSC.JSValue SJ_CustomTypesRepl(JSC.JSValue thisBind, JSC.Arguments args) {
      if(args.Length == 2 && args[1].ValueType == JSC.JSValueType.String) {
        var s = args[1].Value as string;
        if(s != null) {
          if(s.StartsWith("¤BA")) {
            try {
              return new ByteArray(Convert.FromBase64String(s.Substring(3)));
            }
            catch(Exception ex) {
              Log.Warning("ParseJson(" + args[0].ToString() + ", " + s + ") - " + ex.Message);
              return new ByteArray();
            }
          }
          // 2015-09-16T14:15:18.994Z
          if(s.Length == 24 && s[4] == '-' && s[7] == '-' && s[10] == 'T' && s[13] == ':' && s[16] == ':' && s[19] == '.') {
            DateTimeOffset dto;
            if(!DateTimeOffset.TryParseExact(s, "yyyy-MM-ddTH:mm:ss.fffK", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AdjustToUniversal, out dto) ||
                Math.Abs(dto.Year - 1001) < 1) {
              return JSC.JSObject.Marshal(DateTime.Now);
            }
            return JSC.JSValue.Marshal(dto.LocalDateTime);
          }
        }
      }
      return args[1];
    }
    public static JSC.JSValue ParseJson(string json) {
      return JSL.JSON.parse(json, _SJ_Replacer);
    }
    public static string Stringify(JSC.JSValue jv) {
      return JSL.JSON.stringify(jv, null, null, null);
    }

  }
  public class ByteArray : JSI.CustomType {
    private byte[] _val;

    public ByteArray() {
      _val = new byte[0];
    }
    public ByteArray(byte[] data) {
      _val = data;
    }
    public ByteArray(ByteArray src, byte[] data, int pos) {
      if(data == null) {
        return;
      }
      if(src == null) {
        if(pos < 0) {
          pos = 0;
        }
        _val = new byte[pos + data.Length];
        Buffer.BlockCopy(data, 0, _val, pos, data.Length);
      } else {
        if(pos < 0) {  // negative => position from end
          pos = src._val.Length + 1 + pos;
        }
        if(pos >= src._val.Length) {
          _val = new byte[pos + data.Length];
          Buffer.BlockCopy(src._val, 0, _val, 0, src._val.Length);
          Buffer.BlockCopy(data, 0, _val, pos, data.Length);
        } else if(pos == 0) {
          _val = new byte[src._val.Length + data.Length];
          Buffer.BlockCopy(data, 0, _val, 0, data.Length);
          Buffer.BlockCopy(src._val, 0, _val, data.Length, src._val.Length);
        } else {
          _val = new byte[src._val.Length + data.Length];
          Buffer.BlockCopy(src._val, 0, _val, 0, pos);
          Buffer.BlockCopy(data, 0, _val, pos, data.Length);
          Buffer.BlockCopy(src._val, pos, _val, pos + data.Length, src._val.Length - pos);
        }
      }
    }
    public byte[] GetBytes() {
      return _val;
    }

    [JSI.DoNotEnumerate]
    public JSC.JSValue toJSON(JSC.JSValue obj) {
      return new JSL.String("¤BA" + Convert.ToBase64String(_val));
    }
    public override string ToString() {
      return BitConverter.ToString(_val);
    }
  }
  internal class Topic {
    public string path;
    public JSC.JSValue manifest;
    public JSC.JSValue state;
  }
  internal class ArchRecord {
    public DateTime dt;
    public string path;
    public double value;
  }
}
