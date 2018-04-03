1.打发布包
	依赖pom.xml执行maven的install命令，生成独立的执行包: target/springbatch-remote-chunk-0.0.1-SNAPSHOT-executable.zip
2.启动
	解压执行包后，进入解压目录，根据当前是windows系统还是Linux系统分别选择startup.cmd和startup.sh来启动。
	Linux下，注意采用sh startup.sh来启动。
	系统运行时产生的日志在当前启动目录logs。
3.系统中需要对接rocketmq3.2.6版本，具体地址需配置在application.properties中rmq.url项下:替换下地址即可	