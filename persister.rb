require 'kafka'

kafka = Kafka.new(
    seed_brokers: ["localhost:9092"],
    client_id: "miq-persister",
)

consumer = kafka.consumer(group_id: "miq-persisters")
consumer.subscribe("inventory")
ems = ManageIQ::Providers::Openshift::ContainerManager.first

def from_yaml(ems, inv_yaml)
  inv_collections = {}
  raw_collections = YAML.load(inv_yaml)

  raw_collections.each do |collection|
    init_attrs = collection.except(:data)

    # fixup the inv collection
    init_attrs[:parent]      = ems
    init_attrs[:model_class] = init_attrs[:model_class].constantize

    inv_collections[collection[:name]] = ManagerRefresh::InventoryCollection.new init_attrs
  end

  raw_collections.each do |collection|
    inv_collections[collection[:name]].from_raw_data(collection[:data], inv_collections)
  end

  inv_collections
end

puts "Waiting for inventory"
consumer.each_message do |message|
  begin
    inv_collections = from_yaml(ems, message.value)


    puts "Saving Inventory..."
    ManagerRefresh::SaveInventory.save_inventory(ems, inv_collections.values)
    puts "Save Inventory...Complete"
  rescue => err
    puts "#{err}"
    puts "#{err.backtrace.join("\n")}"
  end
end
