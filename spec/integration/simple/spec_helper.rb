require 'spec_helper'

require 'test_cluster'

RSpec.shared_context "a single broker cluster" do
  before(:each) do
    JavaRunner.remove_tmp
    JavaRunner.set_kafka_path!
    $tc = TestCluster.new
    $tc.start
  end

  after(:each) do
    $tc.stop
  end
end
