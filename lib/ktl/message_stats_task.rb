# encoding: utf-8

module Ktl
  class MessageStatsTask
    def initialize(zk_client, shell, topic)
      @zk_client = zk_client
      @shell = shell
      @topic = topic
      @topics = empty_list.send('+=', @topic)
    end

    def execute
      result = []
      total_messages = 0
      brokers = ScalaEnumerable.new(@zk_client.brokers)
      partitions = @zk_client.partitions_for_topics(@topics).apply(@topic)
      topics_partitions = topic_partitions(partitions)
      leader_and_isrs = @zk_client.leader_and_isr_for(topics_partitions.to_set)
      leader_and_isrs.foreach do |r|
        tp, leader_and_isr = r.elements
        leader_id = leader_and_isr.leader_and_isr.leader
        leader = brokers.find { |b| b.id == leader_id }
        consumer = Heller::Consumer.new(leader.connection_string)
        earliest = consumer.earliest_offset(tp.topic, tp.partition)
        latest = consumer.latest_offset(tp.topic, tp.partition)
        result << [tp.topic, tp.partition, earliest, latest, latest - earliest]
        total_messages += result.last.last
      end
      result = result.map do |r|
        r << '(%.2f %%)' % (r.last.fdiv(total_messages) * 100)
      end
      result.unshift(%w[topic partition earliest latest messages])
      @shell.print_table(result)
    end

    private

    def empty_list
      Scala::Collection::Mutable::MutableList.empty
    end

    def topic_partitions(parts)
      topics_partitions = empty_list
      parts.foreach do |partition|
        topics_partitions.send('+=', Kafka::TopicAndPartition.new(@topic, partition))
      end
      topics_partitions
    end
  end
end

