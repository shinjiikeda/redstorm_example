require 'red_storm'

class TestSpout < RedStorm::DSL::Spout
  
  on_init do 
    @count = 0
  end

  on_send :emit => false, :reliable => false do 
    @count += 1
    num = @count
    reliable_emit(@count, num, (num % 10)) 
    sleep 1
  end
  
  on_ack do |msg_id|
    log.info("success: #{msg_id}")
  end

  on_fail do |msg_id|
    log.info("failed: #{msg_id}")
  end
end

class TestBolt1 < RedStorm::DSL::Bolt
  on_receive :anchor => false, :emit => false do |tuple|
    p tuple
    log.info(tuple[0])
    if(tuple[1] != 1)
      anchored_emit(tuple, tuple['num'], tuple['hashkey'])
      ack(tuple)
    else
      fail(tuple)
    end
  end
end

class TestBolt2 < RedStorm::DSL::Bolt
  on_receive :emit => false do |tuple|
    p tuple
    log.info(tuple[0])
    if(tuple[1] != 2)
      ack(tuple)
    else
      fail(tuple)
    end
  end
end

class TestTopology < RedStorm::DSL::Topology
  spout TestSpout do
    output_fields :num, :hashkey
  end

  bolt TestBolt1 do
    source TestSpout, :global
    output_fields :num, :hashkey
  end

  bolt TestBolt2, :parallelism => 2 do
    source TestBolt1, :fields => [ :hashkey ]
  end

  configure do |env|
    debug false
    max_task_parallelism 4
    num_workers 4
    max_spout_pending 1000
  end
end
