log = Logger.new(STDOUT)
begin
  log.info("Waiting for inventory")
  ManageIQ::Messaging.logger = log

  client = ManageIQ::Messaging::Client.open(
    :host => "localhost",
    :port => 61616,
    :user => "admin",
    :password => "smartvm",
    :client_ref => "inventory_persister"
  )

  client.subscribe_messages(:service => 'inventory', :limit => 10) do |messages|
    log.info("Received #{messages.count} messages")
    messages.each do |message|
      begin
        persister = ManagerRefresh::Inventory::Persister.from_raw_data(message.payload)

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

  loop do
    sleep(1)
  end
ensure
  client.close if client
end
