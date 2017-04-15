using Exception = System.Exception;

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
