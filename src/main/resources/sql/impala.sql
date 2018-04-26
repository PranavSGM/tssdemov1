use default;
drop table if exists emp1;

CREATE TABLE IF NOT EXISTS emp1 like parquet '/tssdemo2/l2/parquet/emp1.parquet/_metadata' stored as parquet;
LOAD data inpath '/tssdemo2/l2/parquet/emp1.parquet/' into table emp1;