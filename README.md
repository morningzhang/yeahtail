YeahTail
========
YeahTail is a plugin for flume-ng.<br/>
It can be work like linux commander "tail -f" and this plugin with offset for logfile.<br/>

Feathers:<br/>
1.real time transfer you logfile.<br/>
2.if the flume agent breakdown ,it will restart from the last offset position of the logfile.<br/>
3.It can config you file name with regular expression and the date format with expression "${}".<br/>
4.good suport for daily roll log.<br/>



We can use the config like this for test:<br/>

      a1.sources = r1
      a1.sinks = k1
      a1.channels = c1
      
      # Describe/configure the source
      a1.sources.r1.type = org.apache.flume.YeahTail
      a1.sources.r1.logFileName = /Users/zhangliming/Documents/jworkspace/yeahtail/logs/worker-([\\d]+)(-${yyyyMMdd})?.log 
      a1.sources.r1.bufferSize = 409600
      a1.sources.r1.poolSize = 1
      a1.sources.r1.fetchInterval = 100
      
      # Describe the sink
      a1.sinks.k1.type = logger
      
      # Use a channel which buffers events in memory
      a1.channels.c1.type = memory
      a1.channels.c1.capacity = 1000
      a1.channels.c1.transactionCapacity = 100
      
      # Bind the source and sink to the channel
      a1.sources.r1.channels = c1
      a1.sinks.k1.chan
run like this:<br/>
bin/flume-ng agent --conf conf --conf-file conf/flume-conf.properties --name a1 -Dflume.root.logger=INFO,console
<br/>