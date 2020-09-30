## 帮助文档
本脚本用于循环小批量删除或归档数据。

## 重点参数
- batch_scan_rows,每次扫描主键记录的行数，而不是每次结转的行数。
- batch_sleep_seconds, 每操作batch_scan_rows行的休眠时间，每次循环中会按照当前当前操作影响实际行数来计算休眠时长。
- max_query_seconds，在查找最大最小键时使用的查询最大超时时间，为防止查询超时，查询前会设置会话参数max_statement_time和max_execution_time。
- is_dry_run，是否模拟执行。
- is_user_confirm，执行前是否需要用户再次确认，如果模拟执行，不会弹出确认提示。
- force_table_scan，忽略数据条件，遍历全表数据

## 使用建议
### 手动设置数据遍历范围
对应超大表，查询满足条件的最大键和最小键耗时较长，容易对主服务器造成影响，可以直接指定扫描范围，如：
```SQL
mysql_archive.set_loop_range(max_key=10000, min_key=0)
```
### 强制全表扫描
对应超大表，查询满足条件的最大键和最小键耗时较长，容易对主服务器造成影响，如不想手动设置遍历范围，也可以指定参数force_table_scan来进行全表遍历

### 暂停服务
在程序云彩过程中,如需要停止，可以在当前scripts目录下创建stop.txt文件，当本次循环结束后会自动停止。

PS: 不建议直接KILL，虽然在单实例内已使用事务来保证归档数据的一致性。

### 生成SQL脚本
在模拟执行和实际执行中，都会在当前scripts目录下生成脚本，方便用户直接使用该脚本进行执行。

### 并发执行
如果同时运行多个程序，建议将脚本放置到多个目录下，避免日志混乱，同时需考虑服务器的承载能力。

## 删除数据示例
```JS
def delete_demo():
    source_mysql_server = MySQLServer(
        mysql_host="192.168.0.1",
        mysql_port=3306,
        mysql_user="mysql_test_admin",
        mysql_password="mysql_test_admin.com",
        mysql_charset="utf8mb4",
        database_name="testdb",
        connect_timeout=10
    )
    data_condition = "dt<'2020-01-26 21:49:35' and dt>='2020-08-20 10:57:10'"
    source_table_name = "tb1001"
    mysql_deleter = MyDeleter(
        source_mysql_server=source_mysql_server,
        source_table_name=source_table_name,
        data_condition=data_condition,
        batch_scan_rows=10000,
        batch_sleep_seconds=1,
        max_query_seconds=100,
        is_dry_run=True,
        is_user_confirm=False,
        force_table_scan=True
    )
    mysql_deleter.delete_data()
    mysql_deleter.is_dry_run = False
    mysql_deleter.is_user_confirm = True
    mysql_deleter.delete_data()

```

## 归档数据至本地实例
```JS

def archive_local_demo():
    source_mysql_server = MySQLServer(
        mysql_host="192.168.0.1",
        mysql_port=3306,
        mysql_user="mysql_test_admin",
        mysql_password="mysql_test_admin.com",
        mysql_charset="utf8mb4",
        database_name="testdb",
        connect_timeout=10
    )
    source_table_name = "tb1001"
    target_database_name = source_mysql_server.database_name + "_his"
    target_table_name = source_table_name + "_his"
    data_condition = "dt<'2020-08-20 13:22:30' and dt>='2020-08-20 10:57:10'"
    mysql_archive = MyLocalArchive(
        source_mysql_server=source_mysql_server,
        source_table_name=source_table_name,
        target_database_name=target_database_name,
        target_table_name=target_table_name,
        data_condition=data_condition,
        batch_scan_rows=10000,
        batch_sleep_seconds=1,
        max_query_seconds=100,
        is_dry_run=True,
        is_user_confirm=False
    )
    mysql_archive.archive_data()
    mysql_archive.is_dry_run = False
    mysql_archive.is_user_confirm = True
    mysql_archive.archive_data()

```

## 归档数据至异地实例
```JS

def archive_remote_demo():
    source_mysql_server = MySQLServer(
        mysql_host="192.168.0.1",
        mysql_port=3306,
        mysql_user="mysql_test_admin",
        mysql_password="mysql_test_admin.com",
        mysql_charset="utf8mb4",
        database_name="testdb",
        connect_timeout=10
    )
    target_mysql_server = MySQLServer(
        mysql_host="192.168.0.1",
        mysql_port=3306,
        mysql_user="mysql_test_admin",
        mysql_password="mysql_test_admin.com",
        mysql_charset="utf8mb4",
        database_name="testdb_his",
        connect_timeout=10
    )
    source_table_name = "tb1001"
    target_table_name = source_table_name + "_his"
    data_condition = "dt<'2020-08-20 13:22:30' and dt>='2020-08-20 10:57:10'"
    mysql_archive = MyRemoteArchive(
        source_mysql_server=source_mysql_server,
        source_table_name=source_table_name,
        target_mysql_server=target_mysql_server,
        target_table_name=target_table_name,
        data_condition=data_condition,
        batch_scan_rows=10000,
        batch_sleep_seconds=1,
        max_query_seconds=100,
        is_dry_run=True,
        is_user_confirm=False
    )
    mysql_archive.archive_data()
    mysql_archive.is_dry_run = False
    mysql_archive.is_user_confirm = True
    mysql_archive.archive_data()

```