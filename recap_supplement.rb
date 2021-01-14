require 'json'
require 'faraday'
require 'set'

def valid_ascii_for_xml(string)
  string.force_encoding("ascii").encode("UTF-8", {:invalid => :replace, :replace => ''}).encode(:xml => :text) unless string.nil?
end

def customer_code_convert(string)
  case string
    when /^rcpx[a-z]/
      cust_code = 'PG'
    when /^rcp[a-wy-z][a-z]/
      cust_code = string.gsub(/(^rcp)([a-z]{2})/, '\2').chomp.upcase
    else
      cust_code = ''
  end
end

# exclude from lib_reports
def get_bib(bib_id, conn)
  query = %Q(
        SELECT record_segment
        FROM bib_data
        WHERE bib_id=:id
        ORDER BY seqnum
        )
  segments = []
  cursor = conn.parse(query)
  cursor.bind_param(':id', bib_id)
  cursor.exec()
  while row = cursor.fetch
    segments << row.first
  end
  cursor.close()
  MARC::Reader.decode(segments.join(''), :external_encoding => "UTF-8", :invalid => :replace, :replace => '') unless segments.empty?
end

# exclude from lib_reports
def get_holding(mfhd_id, conn)
  query = %Q(
        SELECT record_segment
        FROM mfhd_data
        WHERE mfhd_id=:id
        ORDER BY seqnum
        )
  segments = []
  cursor = conn.parse(query)
  cursor.bind_param(':id', mfhd_id)
  cursor.exec()
  while row = cursor.fetch
    segments << row.first
  end
  cursor.close()
  MARC::Reader.decode(segments.join(''), :external_encoding => "UTF-8", :invalid => :replace, :replace => '') unless segments.empty?
end

def ids_from_barcode(barcode, conn)
  query = %Q(
    SELECT
      bib_mfhd.bib_id,
      mfhd_item.mfhd_id,
      item_barcode.item_id
    FROM item_barcode
      JOIN mfhd_item
        ON item_barcode.item_id = mfhd_item.item_id
      JOIN bib_mfhd
        ON mfhd_item.mfhd_id = bib_mfhd.mfhd_id
    WHERE
      item_barcode.barcode_status = 1
      AND item_barcode.item_barcode = :barcode
    GROUP BY
      bib_mfhd.bib_id,
      mfhd_item.mfhd_id,
      item_barcode.item_id
  )
  record_ids = Set.new
  cursor = conn.parse(query)
  cursor.bind_param(':barcode', barcode)
  cursor.exec()
  while row = cursor.fetch
    id_hash = {}
    id_hash[:bib_id] = row.shift.to_s
    id_hash[:mfhd_id] = row.shift.to_s
    id_hash[:item_id] = row.shift.to_s
    record_ids << id_hash
  end
  cursor.close()
  record_ids
end

# exclude from lib_reports
def get_item(item_id, conn)
  query = %Q(
        SELECT
          item.item_id,
          item_status_type.item_status_desc,
          item.copy_number,
          temp_loc.location_code,
          perm_loc.location_code,
          mfhd_item.item_enum,
          mfhd_item.chron,
          item_barcode.item_barcode
        FROM item
          INNER JOIN location perm_loc
            ON perm_loc.location_id = item.perm_location
          LEFT JOIN location temp_loc
            ON temp_loc.location_id = item.temp_location
          INNER JOIN item_status
            ON item_status.item_id = item.item_id
          INNER JOIN item_status_type
            ON item_status_type.item_status_type = item_status.item_status
          INNER JOIN mfhd_item
            ON mfhd_item.item_id = item.item_id
          INNER JOIN item_barcode
            ON item_barcode.item_id = item.item_id
        WHERE item.item_id=:item_id AND
          item_status.item_status NOT IN ('5', '6', '16', '19', '20', '21', '23', '24') AND
          item_barcode.barcode_status = 1
        )
  info = {}
  cursor = conn.parse(query)
  cursor.bind_param(':item_id', item_id)
  cursor.exec()
  row = cursor.fetch
  cursor.close()
  info[:id] = row.shift
  info[:status] = row.shift
  info[:copy_number] = row.shift
  info[:temp_location] = row.shift
  info[:perm_location] = row.shift
  enum = row.shift
  info[:enum] = valid_ascii_for_xml(enum)
  chron = row.shift
  info[:chron] = valid_ascii_for_xml(chron)
  info[:barcode] = row.shift
  info
end

# need to refactor to allow a URL to be passed into the method to stop
#   relying on environment variables
def scsb_conn
  Faraday.new(url: SCSB_URL) do |faraday|
    faraday.request   :url_encoded
    faraday.response  :logger
    faraday.adapter   Faraday.default_adapter
  end
end

# exclude from lib_reports in favor of refactor of scsb_conn
def scsb_test_conn
  conn = Faraday.new(url: SCSB_TEST_URL) do |faraday|
    faraday.request  :url_encoded
    faraday.response :logger
    faraday.adapter  Faraday.default_adapter
  end
  conn
end

def parse_scsb_response(response)
  parsed = response.status == 200 ? JSON.parse(response.body) : {}
end

# exclude from lib_reports
def scsb_checkin(barcodes, api_key = nil, conn = nil)
  conn = scsb_conn if conn.nil?
  api_key = SCSB_API_KEY if api_key.nil?
  response = conn.post do |req|
    req.url 'requestItem/checkinItem'
    req.headers['Content-Type'] = 'application/json'
    req.headers['Accept'] = 'application/json'
    req.headers['api_key'] = api_key
    req.body = scsb_checkin_body(barcodes).to_json
  end
  parse_scsb_response(response)
end

# exclude from lib_reports
def scsb_checkin_body(barcodes)
  {
    itemBarcodes: barcodes,
    itemOwningInstitution: 'PUL'
  }
end

def scsb_status(barcodes, api_key = nil, conn = nil)
  conn = scsb_conn if conn.nil?
  api_key = SCSB_API_KEY if api_key.nil?
  response = conn.post do |req|
    req.url 'sharedCollection/itemAvailabilityStatus'
    req.headers['Content-Type'] = 'application/json'
    req.headers['Accept'] = 'application/json'
    req.headers['api_key'] = api_key
    req.body = scsb_status_body(barcodes).to_json
  end
  parse_scsb_response(response)
end

def scsb_status_body(barcodes)
  {
    barcodes: barcodes
  }
end

def scsb_item_by_barcode(barcode, institutions = ['PUL'], api_key = nil, conn = nil)
  conn = scsb_conn if conn.nil?
  api_key = SCSB_API_KEY if api_key.nil?
  response = conn.post do |req|
    req.url '/searchService/search'
    req.headers['Content-Type'] = 'application/json'
    req.headers['Accept'] = 'application/json'
    req.headers['api_key'] = api_key
    req.body = scsb_search_body(barcode, institutions).to_json
  end
  parse_scsb_response(response)
end

def scsb_search_body(barcode, institutions)
  {
    fieldValue: barcode,
    fieldName: 'Barcode',
    owningInstitutions: institutions
  }
end

def scsb_search_cgd_body(cgd, page)
  {
    owningInstitutions:           ['PUL'],
    collectionGroupDesignations:  [cgd],
    pageSize:                     1000,
    pageNumber:                   page
  }
end

def scsb_items_by_cgd(cgd, page, api_key = nil, conn = nil)
  api_key = SCSB_API_KEY if api_key.nil?
  conn = scsb_conn if conn.nil?
  response = conn.post do |req|
    req.url '/searchService/search'
    req.headers['Content-Type'] = 'application/json'
    req.headers['Accept'] = 'application/json'
    req.headers['api_key'] = api_key
    req.body = scsb_search_cgd_body(cgd, page).to_json
  end
  parse_scsb_response(response)
end

def get_ids_from_search_response(response)
  record_ids = Set.new
  items = response['searchResultRows']
  return record_ids unless items
  items.each do |item|
    if item['searchItemResultRows'].empty?
      item_hash = {}
      item_hash[:bib_id] = item['owningInstitutionBibId']
      item_hash[:mfhd_id] = item['owningInstitutionHoldingsId']
      item_hash[:item_id] = item['owningInstitutionItemId']
      record_ids << item_hash
    else
      item['searchItemResultRows'].each do |row|
        item_hash = {}
        item_hash[:bib_id] = item['owningInstitutionBibId']
        item_hash[:mfhd_id] = row['owningInstitutionHoldingsId']
        item_hash[:item_id] = row['owningInstitutionItemId']
        record_ids << item_hash
      end
    end
  end
  record_ids
end

# exclude from lib_reports
def scsb_accession(barcode, cust_code, api_key = nil, conn = nil)
  conn = scsb_conn if conn.nil?
  api_key = SCSB_API_KEY if api_key.nil?
  response = conn.post do |req|
    req.url '/sharedCollection/accession'
    req.headers['Content-Type'] = 'application/json'
    req.headers['Accept'] = 'application/json'
    req.headers['api_key'] = api_key
    req.body = [
                {
                  customerCode: cust_code,
                  itemBarcode: barcode
                }
              ].to_json
  end
  parse_scsb_response(response)
end

# exclude from lib_reports
def scsb_submitcoll(records, cgd_protect = true, api_key = nil, conn = nil)
  conn = scsb_conn if conn.nil?
  api_key = SCSB_API_KEY if api_key.nil?
  response = conn.post do |req|
    req.url '/sharedCollection/submitCollection'
    req.headers['Content-Type'] = 'application/json'
    req.headers['Accept'] = 'application/json'
    req.headers['api_key'] = api_key
    req.body = records_to_string(records)
    req.params = { institution: 'PUL', isCGDProtected: cgd_protect }
  end
  parse_scsb_response(response)
end

# exclude from lib_reports
def records_to_string(records)
  output = StringIO.new('a')
  writer = MARC::XMLWriter.new(output)
  records.each do |record|
    writer.write(record)
  end
  writer.close
  output.close
  output.string
end

# exclude from lib_reports
def scsb_transfer(voy_ids, scsb_ids, item = true, api_key = nil, conn = nil)
  conn = scsb_conn if conn.nil?
  api_key = SCSB_API_KEY if api_key.nil?
  response = conn.post do |req|
    req.url '/sharedCollection/transferHoldingsAndItems'
    req.headers['Content-Type'] = 'application/json'
    req.headers['Accept'] = 'application/json'
    req.headers['api_key'] = api_key
    req.body = if item
      scsb_item_transfer_body(voy_ids, scsb_ids).to_json
    else
      scsb_holding_transfer_body(voy_ids, scsb_ids).to_json
    end
  end
  parse_scsb_response(response)
end

# exclude from lib_reports
def scsb_holding_transfer_body(voy_ids, scsb_ids)
  {
    institution: 'PUL',
    holdingTransfers: [
      {
        destination: {
          owningInstitutionBibId: voy_ids[:bib_id],
          owningInstitutionHoldingsId: voy_ids[:mfhd_id]
        },
        source: {
          owningInstitutionBibId: scsb_ids[:bib_id],
          owningInstitutionHoldingsId: scsb_ids[:mfhd_id]
        }
      }
    ]
  }
end

# exclude from lib_reports
def scsb_item_transfer_body(voy_ids, scsb_ids)
  {
    institution: 'PUL',
    itemTransfers: [
      {
        destination: {
          owningInstitutionBibId: voy_ids[:bib_id],
          owningInstitutionHoldingsId: voy_ids[:mfhd_id],
          owningInstitutionItemId: voy_ids[:item_id]
        },
        source: {
          owningInstitutionBibId: scsb_ids[:bib_id],
          owningInstitutionHoldingsId: scsb_ids[:mfhd_id],
          owningInstitutionItemId: scsb_ids[:item_id]
        }
      }
    ]
  }
end

def collect_barcodes(filename)
  barcodes = []
  File.open(filename, 'r') do |input|
    while line = input.gets
      barcodes << line.gsub(/^"([^"]*)".*$/, '\1').chomp
    end
  end
  barcodes.uniq!
  barcodes
end
