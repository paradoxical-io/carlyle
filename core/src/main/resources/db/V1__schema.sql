CREATE TABLE batches (
  batch_id   BIGINT AUTO_INCREMENT NOT NULL,
  is_closed  BIT DEFAULT 0,
  created_at BIGINT                NOT NULL,
  updated_at BIGINT                NOT NULL,

  PRIMARY KEY (batch_id)
);

CREATE TABLE batch_item_groups (
  id         NVARCHAR(255)   NOT NULL,
  batch_id   BIGINT          NOT NULL,
  items      BIGINT UNSIGNED NOT NULL, # 64 bit multiplexed items to help with insertion
  size       BIGINT          NOT NULL,
  version    BIGINT          NOT NULL DEFAULT 0,
  created_at BIGINT          NOT NULL,
  updated_at BIGINT          NOT NULL,

  FOREIGN KEY (batch_id)
  REFERENCES batches (batch_id),

  PRIMARY KEY (id)
);

CREATE INDEX `batch_items_batch_id_idx`
  ON batch_item_groups (batch_id);