require 'integration/multiple_brokers/spec_helper'

# Created because you can't use a block form for receive with and_call_original
# https://github.com/rspec/rspec-mocks/issues/774
RSpec::Matchers.define :a_broker_id_of do |id|
  match { |actual| actual.broker_id == id }
end

RSpec::Matchers.define :a_broker_id_not do |id|
  match { |actual| actual.broker_id != id }
end

RSpec::Matchers.define :needing_metadata do |val|
  match { |actual| actual.send(:needs_metadata?) == val }
end

RSpec.describe "handling failures", :type => :request do
  include_context "a multiple broker cluster"

  describe "metadata failures" do
    before(:each) do
      @messages_to_send = [
        MessageToSend.new("topic1", "hello"),
        MessageToSend.new("topic2", "hello")
      ]
    end

    describe "unable to connect to brokers" do
      before(:each) do
        @p = Producer.new(["localhost:1092","localhost:1093","localhost:1094"], "producer")
      end

      it "triggers callback failures for both topics" do
        expect {
          @p.send_messages(@messages_to_send)
        }.to raise_error(Poseidon::Errors::UnableToFetchMetadata)
      end
    end
  end

  describe "unknown topic" do
    it "receives error callback" do
      pending "need a way to turn off auto-topic creation just for this test"
      @p = Producer.new(["localhost:9092","localhost:9093","localhost:9094"], "producer")

      expect {
        @p.send_messages([MessageToSend.new("imnothere", "hello")])
      }.to raise_error(Poseidon::Errors::UnableToFetchMetadata)
    end
  end

  describe "leader node loss" do
    let(:topic) { "testing" }
    let(:kafka_partitions) { 1 }

    it "is still able to send messages" do
      @p = Producer.new(["localhost:9092","localhost:9093","localhost:9094"], "producer", :required_acks => 1)

      # Send one to force topic creation
      expect(@p.send_messages([MessageToSend.new(topic, "hello")])).to be_truthy

      @producer = @p.instance_variable_get(:@producer)
      @cluster_metadata = @producer.instance_variable_get(:@cluster_metadata)
      topic_metadata = @cluster_metadata.metadata_for_topics([topic])[topic]

      expect(topic_metadata.available_partitions.length).to be(kafka_partitions)

      # Now, lets kill the topic leader
      leader = topic_metadata.available_partitions.first.leader
      broker = $tc.brokers[topic_metadata.available_partitions.first.leader]
      expect(broker.id).to be(leader)

      broker.without_process do
        expect(@cluster_metadata.metadata_for_topics([topic])[topic].available_partitions.first.leader).to be(leader)
        expect(@producer.send(:refresh_interval_elapsed?)).to be_falsy

        # Setup expectations that the consumer updates its info
        expect(@producer).to receive(:ensure_metadata_available_for_topics).with(needing_metadata(false)).ordered.and_call_original
        expect(@producer).to receive(:send_to_broker).with(a_broker_id_of(leader)).ordered.and_call_original

        expect(@producer).to receive(:reset_metadata).ordered.and_call_original
        expect(@producer).to receive(:ensure_metadata_available_for_topics).ordered.and_call_original
        expect(@producer).to receive(:send_to_broker).with(a_broker_id_not(leader)).ordered.and_call_original

        expect(@p.send_messages([MessageToSend.new(topic, "hello")])).to be_truthy
      end
    end
  end

  describe "partition replica loss" do
    let(:topic) { "testing" }
    let(:kafka_partitions) { 1 }

    it "refreshes metadata correctly" do
      @p = Producer.new(["localhost:9092","localhost:9093","localhost:9094"], "producer", :required_acks => 1)

      # Send one to force topic creation
      expect(@p.send_messages([MessageToSend.new(topic, "hello")])).to be_truthy

      @producer = @p.instance_variable_get(:@producer)
      @cluster_metadata = @producer.instance_variable_get(:@cluster_metadata)
      topic_metadata = @cluster_metadata.metadata_for_topics([topic])[topic]

      expect(topic_metadata.available_partitions.length).to be(kafka_partitions)
      expect(topic_metadata.available_partitions.first.error).to be(0)

      # Now, lets kill the topic leader
      partition_metadata = topic_metadata.available_partitions.first
      expect(partition_metadata.replicas.length).to be(2)
      leader = partition_metadata.leader
      replica = (partition_metadata.replicas - [leader]).first

      broker = $tc.brokers[replica]
      expect(broker.id).to_not be(partition_metadata.leader)
      expect(broker.id).to be(replica)

      broker.without_process do
        expect(@cluster_metadata.metadata_for_topics([topic])[topic].available_partitions.first.replicas).to include(replica)
        expect(@producer.send(:refresh_interval_elapsed?)).to be_falsy

        Timecop.travel(@producer.metadata_refresh_interval_ms) do
          expect(@producer.send(:refresh_interval_elapsed?)).to be_truthy

          # Setup expectations that the consumer updates its info
          expect(@producer).to receive(:refresh_metadata).with(Set.new([topic])).ordered.and_call_original
          expect(@producer).to receive(:ensure_metadata_available_for_topics).with(needing_metadata(false)).ordered.and_call_original
          expect(@producer).to receive(:send_to_broker).with(a_broker_id_of(partition_metadata.leader)).ordered.and_call_original

          # Make sure we don't error out
          expect(@producer).to_not receive(:reset_metadata)

          expect(@p.send_messages([MessageToSend.new(topic, "hello")])).to be_truthy

          # Check the valid metadata
          updated_topic_metadata = @cluster_metadata.metadata_for_topics([topic])[topic]
          expect(updated_topic_metadata.available_partitions.length).to be(kafka_partitions)
          expect(updated_topic_metadata.available_partitions.first.leader).to be(leader)
          expect(updated_topic_metadata.available_partitions.first.replicas).to eq([leader])
          expect(updated_topic_metadata.available_partitions.first.error).to be(9)
        end
      end
    end
  end
end
