require 'integration/simple/spec_helper'

RSpec.describe "three brokers in cluster", :type => :request do
  include_context "a single broker cluster"

  describe "sending batches of 1 message" do
    it "sends messages to all brokers" do
    end
  end
end
