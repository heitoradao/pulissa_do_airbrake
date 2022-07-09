module FileHandler
	class Main
		class << self
			def generate_file(file_name:, body:)
				return if body.nil?
				file = File.new("./data/#{file_name}.json", 'w')
        
        file.write(body)
        file.close
			end
		end
	end
end
