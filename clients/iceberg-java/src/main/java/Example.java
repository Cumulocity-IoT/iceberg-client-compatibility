import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.rest.RESTCatalog;

/** Connects to the Iceberg catalog of a Cumulocity tenant and prints a few measurement rows. */
public class Example {

  public static RESTCatalog connect(Map<String, String> extra) {
    Map<String, String> properties = new HashMap<>();
    properties.put("type", "rest");
    properties.put("uri", System.getenv("CATALOG_URI"));
    properties.put("warehouse", System.getenv("CATALOG_WAREHOUSE"));
    properties.put("rest.auth.type", "oauth2");
    properties.put("credential", System.getenv("CATALOG_CREDENTIAL")); // <clientId>:<clientSecret>
    properties.put("oauth2-server-uri", System.getenv("CATALOG_TOKEN_URI"));
    properties.put("scope", System.getenv("CATALOG_SCOPE")); // PRINCIPAL_ROLE:ALL
    properties.put("header.X-Iceberg-Access-Delegation", "vended-credentials");
    properties.putAll(extra);

    RESTCatalog catalog = new RESTCatalog();
    catalog.initialize("lake", properties);
    return catalog;
  }

  public static void main(String[] args) throws Exception {
    try (RESTCatalog catalog = connect(Map.of())) {
      Table table = catalog.loadTable(TableIdentifier.of("cdc_measurement", "c8y_Temperature"));
      try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
        int n = 0;
        for (Record row : rows) {
          System.out.println(row);
          if (++n == 5) {
            break;
          }
        }
      }
    }
  }
}
