# flink-basic
This is a flink tutorial project 


### flink 本地集群模式运行命令
    + ./bin/start-cluster.sh
    + ./bin/stop-cluster.sh

    1. 任务提交
        通过flink web ui页面上传jar包进行提交
        通过命令行方式提交命令
        ./bin/flink run examples/streaming/WrodCount.jar  （jar包中包含了主入口类）
        ./bin/flink run -c a.b.c.XXX /path/to/applicationJar --hostname aaa --port 9999

    2. 任务取消
        通过flink web ui页面点击Cancel按钮取消任务
        通过命令行查看任务信息
            ./bin/flink list -a  查看所以任务信息
            ./bin/flink list -r  查看运行任务的信息
            ./bin/flink list -s  查到调度任务的信息
        
        取消任务：
            ./bin/flink cancal <jobID>