require 'kafka'

kafka = Kafka.new(
    seed_brokers: ["localhost:9092"],
    client_id: "miq-persister",
)

consumer = kafka.consumer(group_id: "miq-persisters")
consumer.subscribe("inventory")

def from_yaml(inv_yaml)
  persister = ManagerRefresh::Inventory::Persister.from_yaml(inv_yaml)
  return persister.manager, persister.collections
end

puts "Waiting for inventory"
consumer.each_message do |message|
  begin
    ems, inv_collections = from_yaml(message.value)

    puts "Saving Inventory..."
    ManagerRefresh::SaveInventory.save_inventory(ems, inv_collections.values)
    puts "Save Inventory...Complete"
  rescue => err
    puts "#{err}"
    puts "#{err.backtrace.join("\n")}"
  end
end
