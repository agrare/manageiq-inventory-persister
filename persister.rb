#!/usr/bin/env ruby

if !defined?(Rails)
  ENV["RAILS_ROOT"] ||= File.expand_path("../manageiq", __dir__)
  require File.expand_path("config/environment", ENV["RAILS_ROOT"])
end

require "trollop"
require "manageiq-messaging"

Thread.abort_on_exception = true

def main(args)
  log = Logger.new(STDOUT)

  ManageIQ::Messaging.logger = log if args[:debug]

  log.info("Connecting...")
  client = ManageIQ::Messaging::Client.open(
    :host => "localhost",
    :port => 61616,
    :user => "admin",
    :password => "smartvm",
    :client_ref => "inventory_persister"
  )
  log.info("Connected.")

  log.info("Waiting for inventory...")

  client.subscribe_messages(:service => "inventory", :limit => 10) do |messages|
    messages.each do |message|
      begin
        persister = ManagerRefresh::Inventory::Persister.from_yaml(message.payload)

        log.info("Saving Inventory...")
        ManagerRefresh::SaveInventory.save_inventory(persister.manager, persister.inventory_collections)
        log.info("Save Inventory...Complete")
      rescue => err
        log.error("#{err}")
        log.error("#{err.backtrace.join("\n")}")
      ensure
        client.ack(message.ack_ref)
      end
    end
  end

  loop { sleep 1 }
ensure
  client.close if client
end

def parse_args
  args = Trollop.options do
    opt :q_hostname, "queue hostname", :type => :string
    opt :q_port,     "queue port",     :type => :integer
    opt :q_user,     "queue username", :type => :string
    opt :q_password, "queue password", :type => :string
    opt :debug,      "debug", :type => :flag
  end

  args[:q_hostname]   ||= ENV["QUEUE_HOSTNAME"] || "localhost"
  args[:q_port]       ||= ENV["QUEUE_PORT"]     || "61616"
  args[:q_user]       ||= ENV["QUEUE_USER"]     || "admin"
  args[:q_password]   ||= ENV["QUEUE_PASSWORD"] || "smartvm"

  args[:q_port] = args[:q_port].to_i

  # %i(q_hostname q_port q_user q_password).each do |param|
  #   raise Trollop::CommandlineError, "--#{param} required" if args[param].nil?
  # end

  args
end

args = parse_args

main args
