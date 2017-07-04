using Exception = System.Exception;
using System;

namespace SHS {
  public class UrlUtils {
    public static string InvertHost(string url) {
      int a = PrefixLength(url);
      int b = HostLength(url, a);
      return url.Substring(0, a)
        + HostUtils.InvertHost(url.Substring(a, b))
        + url.Substring(a+b);
    }

    public static string HostOf(string url) {
      int a = PrefixLength(url);
      int b = HostLength(url, a);
      return url.Substring(a, b);
    }

    public static string DomainOf(string url) {
      return HostUtils.DomainOf(UrlUtils.HostOf(url));
    }

    private static int PrefixLength(string url) {
      if (url.SafeStartsWith("http://")) {
        return 7;
      } else if (url.SafeStartsWith("https://")) {
        return 8;
      } else {
        throw new Exception("URL " + url + " does not start with http:// or https://");
      }
    }

    private static int HostLength(string url, int prefixLen) {
      int i = prefixLen;
      while (i < url.Length && url[i] != ':' && url[i] != '/') i++;
      return i - prefixLen;
    }

    public static string getNormalURLFormat(string decodeURL)
    {
        char[] delimiter = { ' ', '\t', ','};
        string first = "", later = "", revfirst = "";
        int split_pos = decodeURL.IndexOf(')');
        if (split_pos >= 4)
        {
            first = decodeURL.Substring(0, split_pos);
            string[] dn = first.Split(delimiter, StringSplitOptions.RemoveEmptyEntries);
            int len = dn.Length;
            revfirst = dn[0];
            for (int i = 1; i < len; i++)
            {
                revfirst = dn[i] + "." + revfirst;
            }
            later = decodeURL.Substring(split_pos + 1);
            return ("http://" + revfirst + later);
        }
        return null;
    }
  }

  public static class ExtensionMethods {
    public static bool SafeStartsWith(this string full, string pref) {
      if (full.Length < pref.Length) return false;
      for (int i = 0; i < pref.Length; i++) {
        if (full[i] != pref[i]) return false;
      }
      return true;
    }
  }
}
