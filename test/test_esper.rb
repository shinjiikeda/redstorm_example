require 'red_storm'

class TestSpout < RedStorm::DSL::Spout
  
  on_init do 
    @count = 0
  end

  on_send :emit => false, :reliable => false do 
    @count += 1
    num = rand(1000)
    reliable_emit(@count, num) 
    sleep 1
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
    map.put("val", java.lang.Long.java_class)
    config.addEventType("SampleEvent", map)
    
    @serv = EPServiceProviderManager.getDefaultProvider(config)
    st = @serv.getEPAdministrator().createEPL("select avg(val) from SampleEvent.win:time( 60 sec ) output first every 60 sec ")
    
    listener = EsperListener.new
    st.addListener(listener)

  end

  on_receive :anchor => false, :emit => false do |tuple|
    p tuple
    log.info(tuple[0])

    event = java.util.HashMap.new
    event.put("val", tuple[0])
    @serv.getEPRuntime.sendEvent(event, "SampleEvent")
    
    anchored_emit(tuple, tuple[0])
    ack(tuple)
  end
end

class TestTopology < RedStorm::DSL::Topology
  spout TestSpout do
    output_fields :num
  end

  bolt TestBolt do
    source TestSpout, :global
  end

  configure do |env|
    debug false
    max_task_parallelism 4
    num_workers 4
    max_spout_pending 1000
  end
end
