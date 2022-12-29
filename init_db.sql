CREATE TABLE machine
(
  id                 VARCHAR(36) NOT NULL PRIMARY KEY COMMENT 'id',
  main_ip            VARCHAR(15) COMMENT '主ip',
  device_system_info JSON COMMENT '设备信息',
  system_info        JSON COMMENT '系统信息',
  cpu_info           JSON COMMENT 'CPU信息',
  memory_info        JSON COMMENT '内存信息',
  load_avg           JSON COMMENT '负载信息',
  interfaces         JSON COMMENT '网卡信息'
);