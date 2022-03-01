TODO
-----
#### UI
* User login / security
* Tree view: remove dummy root node
* Backfill wizard
* Fix datepicker
* Confirm message on clear with list of tasks instances

#### Write unittests
* For each existing operator

#### More Operators!
* HIVE
* BaseDataTransferOperator
* File2MySqlOperator
* PythonOperator
* DagTaskSensor for cross dag dependencies
* PIG

#### Macros
* Previous execution timestamp
* ...

#### Backend
* CeleryExecutor
* Master to derive BaseJob
* Clear should kill running jobs
* Mysql port should carry through
* Command line, confirm on clear, show list

#### Misc
* Write an hypervisor, looks for dead jobs without a heartbeat and kills
* Authentication with Flask-Login and Flask-Principal
* email_on_retry

#### Wishlist
* Support for cron like synthax (0 * * * ) using croniter library
