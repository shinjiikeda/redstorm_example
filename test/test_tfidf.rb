require 'red_storm'

#
# streamingでのtfidf計算テスト
# 単位時間ごとにdfを計算
# まだ書きかけ
#
TIME_INTERVAL = 10 # sec
INPUT_FILE = "test.json"

class TestSpout < RedStorm::DSL::Spout
  
  on_init do 
    # init
    @doc_id = 0
  end

  on_send :emit => false, :reliable => false do 
    @doc_id += 1
    text = ""
    100.times.each do | n |
      text << "#{rand(100).to_s} " 
    end
    text << "#{rand(100)} " * rand(5)
    p text 
    unreliable_emit(@doc_id, text)
    sleep 0.5
  end
  
  on_ack do |msg_id|
    log.info("success: #{msg_id}")
  end

  on_fail do |msg_id|
    log.info("fail: #{msg_id}")
  end
end

class SplitBolt < RedStorm::DSL::Bolt
  on_receive :anchor => false, :emit => false do |tuple|
    # log.info(tuple[0])
    doc_id = tuple[0]
    words = tuple[1].split(/[,\.\s]+/)
    num_words = words.size
    h = {}
    words.each do | word |
      if h.has_key?(word)
        h[word] +=1
      else
        h[word] = 1
      end
    end
    
    h.each do |word, count|
      unanchored_emit(doc_id, word, count)
    end
  end
end

class CalcBolt < RedStorm::DSL::Bolt
  on_init do
    @df = {}
    @df_before = nil
    @df_utime = Time.now.to_i
  end
  
  on_receive :anchor => false, :emit => false do |tuple|
    doc_id = tuple[0]
    word = tuple[1]
    tf = tuple[2]
    if @df.has_key?(word)
      @df[word] += 1
    else
      @df[word] = 1
    end
    
    if ! @df_before.nil?
      tf_idf = tf.to_f / @df_before[word]
      unanchored_emit(doc_id, word, tf_idf)
    end
    
    now = Time.now.to_i
    if now > @df_utime + TIME_INTERVAL
      @df_before = @df
      @df = {}
      @df_utime = now
    end
  end
end

class OutputBolt < RedStorm::DSL::Bolt
  on_init do
    @count = 0
    @queue = []
    @avg = 0
    @std = 0
  end

  on_receive :anchor => false, :emit => false do |tuple|
    @count += 1
    doc_id = tuple[0]
    word = tuple[1]
    @queue.push(tuple[2])
    if @queue.size > 10000
      @queue.pop
    end
    if @count % 10 == 1
      @avg = @queue.inject(0.0){|r,i| r+=i }/@queue.size
      @std = Math.sqrt(@queue.inject(0.0){|r,i| r+=(i - @avg)*(i - @avg)}/ @queue.size)
    end
    t = 10*(tuple[2] - @avg)/@std + 50
    log.info("#{@count} doc_id: #{doc_id}, word:#{word}, tf_idf:#{tuple[2]} avg:#{@avg}, std:#{@std}, T:#{t}") if t >= 60
  end
end

class TFIDFTopology < RedStorm::DSL::Topology
  spout TestSpout do
    output_fields :doc_id, :text
  end

  bolt SplitBolt do
    source TestSpout, :global
    output_fields :doc_id, :word, :tf
  end

  bolt CalcBolt, :parallelism => 2 do
    source SplitBolt, :fields => [ :doc_id, :word ]
    output_fields :doc_id, :word, :tf_idf
  end

  bolt OutputBolt, :parallelism => 2 do
    source CalcBolt, :global
  end

  configure do |env|
    debug false
    max_task_parallelism 4
    num_workers 4
    max_spout_pending 1000
  end
end
