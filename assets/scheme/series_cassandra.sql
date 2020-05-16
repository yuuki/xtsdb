CREATE TABLE datapoint (
	"metric_id" VARCHAR,
	"timestamp" TIMESTAMP,
	"value"     DOUBLE,

	PRIMARY KEY ("metric_id", "timestamp"),
);
