CREATE TABLE datapoint (
	"metric_id" BLOB,
	"timestamp" TIMESTAMP,
	"values"    BLOB,

	PRIMARY KEY ("metric_id", "timestamp"),
);
