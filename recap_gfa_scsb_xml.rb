require 'mysql2'
require 'marc'
require 'oci8'
require_relative './../tmp/credentials'
require_relative './../tmp/barcodes'
require_relative './recap_supplement'
puts "Began at #{Time.now.strftime("%H:%M:%S")}"
shared_codes = ['PA', 'GP', 'QK', 'PF']
client = Mysql2::Client.new(:host => 'localhost', :username => RECAP_USER, :password => RECAP_PASS, :database => RECAP_DATABASE)
bib_count = `wc -l recap_bibs.txt`.to_i
total_files = (bib_count/50000.0).ceil
first_file = 4
last_file = 4
file_num = first_file-1
conn = OCI8.new(USER,PASS,NAME)
loop do
  file_num += 1
  break if file_num > last_file
  File.open("/home/mzelesky/marc_cleanup/logs/recap_extract/recap_records_#{file_num}.xml", 'a') do |output|
    filename = "bibs#{file_num.to_s.rjust(2, '0')}"
    bib_count = `wc -l #{filename}`.to_i
    mfhd_count = 0
    item_count = 0
    output.puts("<bibRecords>")
    bib_input = File.open(filename, 'r')
    while line = bib_input.gets
      bib = line.chomp
      bib_info = get_bib(bib, conn)
      if bib_info
        output.write("<bibRecord><bib><owningInstitutionId>PUL<\/owningInstitutionId>")
        output.write("<content>")
        output.write("<collection xmlns='http:\/\/www\.loc\.gov\/MARC21\/slim'>")
        output.write(bib_info.to_xml.to_s.gsub(/ xmlns='http:\/\/www.loc.gov\/MARC21\/slim'/, ''))
        output.write("<\/collection><\/content><\/bib>")
        output.write("<holdings>")
        mfhd_ids = []
        client.query("SELECT mfhd_id FROM record_ids where bib_id = #{bib} GROUP BY mfhd_id ORDER BY mfhd_id").each do |row|
          mfhd_ids.push(row["mfhd_id"])
        end
        mfhd_ids.each do |mfhd|
          mfhd_count += 1 
          mfhd_info = get_holding(mfhd, conn)
          if mfhd_info
            mfhd_location = mfhd_info['852']['b']
            output.write("<holding>")
            mfhd_id = mfhd_info['001'].value
            output.write("<owningInstitutionHoldingsId>#{mfhd_id}<\/owningInstitutionHoldingsId>")
            output.write("<content>")
            output.write("<collection xmlns='http:\/\/www\.loc\.gov\/MARC21\/slim'>")
            output.write("<record>")
            output.write("<datafield ind1='#{mfhd_info['852'].indicator1}' ind2='#{mfhd_info['852'].indicator2}' tag='852'>")
            output.write("<subfield code='b'>#{mfhd_location}<\/subfield>")
            if mfhd_info['852']['h']
              output.write("<subfield code='h'>#{mfhd_info['852']['h'].encode(:xml => :text)}")
              if mfhd_info['852']['i']
                output.write(mfhd_info['852']['i'].encode(:xml => :text))
              end      
              output.write("<\/subfield>")
            end
            output.write("<\/datafield>")
            if mfhd_info['866']
              mfhd_866 = mfhd_info.to_xml.to_s.scan(/<datafield ind1='.' ind2='.' tag='866'>(?:<subfield code='.'>[^<]*<\/subfield>)*<\/datafield>/)
              mfhd_866.each do |field|
                output.write(field)
              end
            end
            output.write("<\/record>")
            output.write("<\/collection><\/content>")
            output.write("<items><content><collection xmlns='http:\/\/www\.loc\.gov\/MARC21\/slim'>")
            item_ids = []
            client.query("SELECT item_id FROM record_ids where mfhd_id = #{mfhd} GROUP BY item_id ORDER BY item_id").each do |row|
              item_ids.push(row["item_id"])
            end
            item_ids.each do |item|
              item_count += 1
              item_info = get_item(item, conn)
              customer_code = BARCODES[item_info[:barcode]]
              output.write("<record><datafield ind1='0' ind2='0' tag='876'>")            
              output.write("<subfield code='a'>#{item_info[:id]}<\/subfield>")
              output.write("<subfield code='h'>")
              case customer_code
                when 'PJ', 'PK', 'PL', 'PM', 'PN', 'PT'
                  output.write("In Library Use")
                when 'PB', 'PH', 'PS', 'PW', 'PZ', 'PG'
                  output.write("Supervised Use")
                else
                  output.write(" ")
              end
              output.write("<\/subfield>")
              case item_info[:status]
                when 'Not Charged'
                  output.write("<subfield code='j'>Available<\/subfield>")
                when 'Charged', 'Renewed', 'Overdue', 'On Hold', 'In Transit', 'In Transit On Hold', 'Remote Storage Request'
                  output.write("<subfield code='j'>Loaned<\/subfield>")
                else
                  output.write("<subfield code='j'>Not Available<\/subfield>")
              end
              output.write("<subfield code='p'>#{item_info[:barcode]}<\/subfield>")
              output.write("<subfield code='t'>#{item_info[:copy_number]}<\/subfield>")
              output.write("<subfield code='3'>")
              if item_info[:enum] && item_info[:chron]
                output.write(item_info[:enum].force_encoding("ascii").encode("UTF-8", {:xml => :text, :invalid => :replace, :replace => ''}) + " \(#{item_info[:chron]}\)")
              elsif item_info[:enum]
                output.write(item_info[:enum].force_encoding("ascii").encode("UTF-8", {:xml => :text, :invalid => :replace, :replace => ''}))
              elsif item_info[:chron]
                output.write("\(" + item_info[:chron].force_encoding("ascii").encode("UTF-8", {:xml => :text, :invalid => :replace, :replace => ''}) + "\)")
              end
              output.write("<\/subfield>")
              output.write("<\/datafield>")
              output.write("<datafield ind1='0' ind2='0' tag='900'>")
              mfhd_cust_code = customer_code_convert(mfhd_location)
              perm_loc_cust_code = customer_code_convert(item_info[:perm_location])
              temp_loc_cust_code = if item_info[:temp_location]
                customer_code_convert(item_info[:temp_location])
              else
                ''
              end
              if mfhd_cust_code == customer_code && perm_loc_cust_code == customer_code && (temp_loc_cust_code == customer_code || temp_loc_cust_code == '')
                if shared_codes.include?(customer_code)
                  output.write("<subfield code='a'>Shared<\/subfield>")
                else
                  output.write("<subfield code='a'>Private<\/subfield>")
                end
              else
                output.write("<subfield code='a'>Private<\/subfield>")
              end
              output.write("<subfield code='b'>#{customer_code}<\/subfield>")
              output.write("<\/datafield><\/record>")
            end
      	    output.write("<\/collection><\/content><\/items><\/holding>")
          end
        end
      output.write("<\/holdings>")
      output.puts("<\/bibRecord>")
      end
    end
    output.puts("<\/bibRecords>")
    output.close  
    File.open("/home/mzelesky/marc_cleanup/logs/recap_extract/recap_records_#{file_num}.log", 'a') do |output|
      output.write("Bibliographic records: ")
      output.puts(bib_count.to_s)
      output.write("Holdings records: ")
      output.puts(mfhd_count.to_s)
      output.write("Item records: ")
      output.puts(item_count.to_s)
    end
  end
end
puts "Completed at #{Time.now.strftime("%H:%M:%S")}"
