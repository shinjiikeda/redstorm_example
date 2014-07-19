require 'red_storm'

class TestSpout < RedStorm::DSL::Spout
  
  on_init do 
    @count = 0
  end

  on_send :emit => false, :reliable => false do 
    @count += 1
    num = rand(1000)
    reliable_emit(@count, @count % 10, num) 
    sleep 0.01
  end
  
  on_ack do |msg_id|
    log.info("success: #{msg_id}")
  end

  on_fail do |msg_id|
    log.info("failed: #{msg_id}")
  end
end

class TestBolt < RedStorm::DSL::Bolt
  on_init do 
    log.info("init esper start.")
    require 'java'
    
    java_import 'com.espertech.esper.client.Configuration'
    java_import 'com.espertech.esper.client.EPServiceProvider'
    java_import 'com.espertech.esper.client.EPServiceProviderManager'
    java_import 'com.espertech.esper.client.EPStatement'
    java_import 'com.espertech.esper.client.UpdateListener'
    java_import 'java.util.HashMap'
  
    class EsperListener
      include UpdateListener

      def initialize()
        puts "Initialized EsperListener"
      end
  
      def update(newEvents, oldEvents)
        newEvents.each do |event|
          puts "New event: #{event.getUnderlying.toString}"
        end
      end
    end

    config = com.espertech.esper.client.Configuration.new
    
    map = java.util.HashMap.new
    map.put("key", java.lang.String.java_class)
    map.put("val", java.lang.Long.java_class)
    config.addEventType("SampleEvent", map)
    
    @serv = EPServiceProviderManager.getDefaultProvider(config)
    begin 
      sql = "select key, avg(val) from SampleEvent.win:time( 60 sec ) group by key  output all every 60 sec"
      st = @serv.getEPAdministrator().createEPL(sql)
    
      listener = EsperListener.new
      st.addListener(listener)
    rescue
      log.error("error")
    end
    log.info("init esper finish.")
  end

  on_receive :anchor => false, :emit => false do |tuple|
    p tuple
    key = tuple[0]
    val = tuple[1]

    event = java.util.HashMap.new
    event.put("key", key)
    event.put("val", val)
    @serv.getEPRuntime.sendEvent(event, "SampleEvent")
    
    anchored_emit(tuple, tuple[0])
    ack(tuple)
  end
end

class TestTopology < RedStorm::DSL::Topology
  spout TestSpout do
    output_fields :key, :val
  end

  bolt TestBolt, :parallelism => 2 do
    source TestSpout, :fields => [ :key ]
  end

  configure do |env|
    debug false
    max_task_parallelism 4
    num_workers 4
    max_spout_pending 1000
  end
end
