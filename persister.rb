def from_yaml(inv_yaml)
  persister = ManagerRefresh::Inventory::Persister.from_yaml(inv_yaml)
  return persister.manager, persister.collections
end

puts "Waiting for inventory"
client = ActiveMqClient.open(true)
MiqQueue.subscribe_job(client, :service => 'ems_inventory') do |_sender, _mtype, message|
  begin
    persister = ManagerRefresh::Inventory::Persister.from_raw_data(message)

    puts "Saving Inventory..."
    ManagerRefresh::SaveInventory.save_inventory(persister.manager, persister.inventory_collections)
    puts "Save Inventory...Complete"
  rescue => err
    puts "#{err}"
    puts "#{err.backtrace.join("\n")}"
  end
end

loop do
  sleep(1)
end
