require 'mysql2'
require 'oci8'
require_relative './../tmp/credentials'
require_relative './../tmp/barcodes'

def barcode_record_ids_location(barcodes)
  barcodes = OCI8::in_cond(:barcodes, barcodes)
  %Q(
    SELECT
      bib_item.bib_id,
      mfhd_item.mfhd_id,
      item_barcode.item_id
    FROM item_barcode
      JOIN mfhd_item
        ON item_barcode.item_id = mfhd_item.item_id
      JOIN bib_item
        ON item_barcode.item_id = bib_item.item_id
    WHERE
      item_barcode.item_barcode IN (#{barcodes.names})
      AND item_barcode.barcode_status = 1
  )
end

client = Mysql2::Client.new(:host => 'localhost', :username => RECAP_USER, :password => RECAP_PASS, :database => RECAP_DATABASE)
client.query("DROP TABLE IF EXISTS record_ids")
client.query("CREATE TABLE record_ids(id INT NOT NULL AUTO_INCREMENT, bib_id INT NOT NULL, mfhd_id INT NOT NULL, item_id INT NOT NULL, PRIMARY KEY (id))")
puts "Loading barcodes"
barcodes = BARCODES.keys
puts "Finished barcodes"
total_loops = (barcodes.count/300.0).ceil
loop_num = 0
conn = OCI8.new(USER,PASS,NAME)
loop do  
  loop_num += 1
  break if loop_num > total_loops
  barcode_segment = barcodes.slice(((loop_num-1)*300)..(((loop_num)*300)-1))
  query = barcode_record_ids_location(barcode_segment)
  conn.exec(query, *barcode_segment) do |row|
    bib_id = row.shift
    mfhd_id = row.shift
    item_id = row.shift
    client.query("INSERT INTO record_ids (bib_id,mfhd_id,item_id) VALUES(#{bib_id}, #{mfhd_id}, #{item_id})")
  end
end
conn.logoff
