CREATE TABLE datapoint (
	"metric_id" VARCHAR,
	"timestamp" TIMESTAMP,
	"values"    BLOB,

	PRIMARY KEY ("metric_id", "timestamp"),
);
