CREATE TABLE IF NOT EXISTS rtb_requests (
  ts         DateTime64(3) DEFAULT now64(3),
  req_uuid   UUID,
  openrtb_id String,
  tmax       UInt16,
  fanout     UInt8
) ENGINE = MergeTree ORDER BY (req_uuid, ts);

CREATE TABLE IF NOT EXISTS rtb_responses (
  ts          DateTime64(3) DEFAULT now64(3),
  req_uuid    UUID,
  target      String,
  status_code UInt16,
  duration_ms UInt32,
  dropped     UInt8,
  resp_body   String
) ENGINE = MergeTree ORDER BY (req_uuid, ts);