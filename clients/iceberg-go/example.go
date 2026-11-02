// Connects Iceberg Go to the lake of a Cumulocity tenant and reads a few rows.
//
// The catalog settings come from the CATALOG_* environment variables (see the README). Storage
// credentials are vended by the catalog, so no S3 keys or region are configured here.
package main

import (
	"context"
	"net/url"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	_ "github.com/apache/iceberg-go/io/gocloud/s3" // registers the s3:// file IO
	"github.com/apache/iceberg-go/table"
)

// connect logs in to the tenant's Iceberg REST catalog with OAuth2 client credentials.
func connect(ctx context.Context, extra ...rest.Option) (*rest.Catalog, error) {
	tokenURI, err := url.Parse(os.Getenv("CATALOG_TOKEN_URI"))
	if err != nil {
		return nil, err
	}
	opts := []rest.Option{
		rest.WithWarehouseLocation(os.Getenv("CATALOG_WAREHOUSE")),
		rest.WithCredential(os.Getenv("CATALOG_CREDENTIAL")), // <clientId>:<clientSecret>
		rest.WithAuthURI(tokenURI),
		rest.WithScope(os.Getenv("CATALOG_SCOPE")), // PRINCIPAL_ROLE:ALL
		rest.WithHeaders(map[string]string{"X-Iceberg-Access-Delegation": "vended-credentials"}),
	}
	return rest.NewCatalog(ctx, "lake", os.Getenv("CATALOG_URI"), append(opts, extra...)...)
}

// readAFewRows reads five rows of a table, for example ("cdc_measurement", "c8y_Temperature").
func readAFewRows(ctx context.Context, cat *rest.Catalog, namespace, name string) (arrow.Table, error) {
	tbl, err := cat.LoadTable(ctx, catalog.ToIdentifier(namespace, name))
	if err != nil {
		return nil, err
	}
	return tbl.Scan(table.WithLimit(5)).ToArrowTable(ctx)
}
