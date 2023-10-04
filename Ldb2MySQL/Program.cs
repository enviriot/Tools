using X13;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySqlConnector;

namespace X13 {
  internal class Program {
    static void Main(string[] args) {
      Log.Info("Start({0}) v.{1}", string.Join(",", args), System.Reflection.Assembly.GetExecutingAssembly().GetName().Version.ToString(4));
      MySQL mdb = null;
      LiteDB ldb = null;
      try {
        if(args.Length == 0 || !Uri.TryCreate(args[0], UriKind.Absolute, out Uri url)) {
          Console.ForegroundColor = ConsoleColor.Yellow;
          Console.WriteLine("Use: Ldb2MySQL tcp://user:pa$w0rd@server/");
          Console.ResetColor();
          Console.Read();
          return;
        }
        mdb = new MySQL(url);
        mdb.Create();
        ldb = new LiteDB();
        ldb.Open();

        foreach(var t in ldb.Topics()) {
          Console.Write("\r" + t.path);
          mdb.Write(t);
        }
        Log.Info("Topics transfer finished");

        Console.WriteLine();
        foreach(var l in ldb.LogRecords()) {
          Console.Write("\r" + l.dt.ToString());
          mdb.Write(l);
        }
        Log.Info("Logs transfer finished");


        Console.WriteLine();
        foreach(var r in ldb.ArchRecords()) {
          Console.Write("\r" + r.dt.ToString());
          mdb.Write(r);
        }
        Log.Info("Arch transfer finished");

        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine("\n\rFinish");
      }
      catch (TaskCanceledException ) {
      
      }
      catch (Exception ex) {
        Log.Error("Exception - {0}", ex.ToString());
      }
      finally {
        mdb?.Dispose();
        ldb?.Dispose();
      }
      Log.Finish();
      Console.ResetColor();
      Console.Read();
    }
  }
}
