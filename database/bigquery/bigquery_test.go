package bigquery

import (
	"fmt"
	"testing"

	"github.com/dhui/dktest"
	"github.com/docker/go-connections/nat"
	"github.com/golang-migrate/migrate/v4/dktesting"

	dt "github.com/golang-migrate/migrate/v4/database/testing"
)

const (
	bqProjectID    = "projectId"
	bqDataset      = "dataset"
	bqEmulatorPort = 9050
)

var (
	opts = dktest.Options{
		PortRequired: true,
		Cmd:          []string{"bigquery-emulator", "--project=projectId", "--dataset=dataset"},
		Platform:     "linux/x86_64",
		ExposedPorts: nat.PortSet{
			nat.Port(fmt.Sprintf("%d/tcp", bqEmulatorPort)): struct{}{},
		},
		ReadyFunc: isReady,
	}

	specs = []dktesting.ContainerSpec{
		{ImageName: "ghcr.io/goccy/bigquery-emulator:0.6.5", Options: opts},
	}
)

func Test(t *testing.T) {
	t.Run("test", test)

	t.Cleanup(func() {
		for _, spec := range specs {
			t.Log("Cleaning up ", spec.ImageName)

			if err := spec.Cleanup(); err != nil {
				t.Error("Error removing ", spec.ImageName, "error:", err)
			}
		}
	})
}

func test(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		endpoint := fmt.Sprintf("http://%s:%s", ip, port)

		addr := fmt.Sprintf("bigquery://%s/%s?x-insecure=true&x-endpoint=%s", bqProjectID, bqDataset, endpoint)

		bq := &BigQuery{}
		d, err := bq.Open(addr)
		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			if err = d.Close(); err != nil {
				t.Error(err)
			}
		}()

		dt.Test(t, d, []byte("SELECT 1"))
	})
}
