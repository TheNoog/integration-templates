url = 'https://randomuser.me/api/';
file_path = 'data.json';

try
    options = weboptions('Timeout', 10); % Set timeout to 10 seconds (optional)
    response = webread(url, options);
    if ~isempty(response)
        json_str = jsonencode(response);
        file = fopen(file_path, 'w');
        fprintf(file, json_str);
        fclose(file);
        disp('File saved successfully.');
    else
        disp('Error: Empty response.');
    end
catch ME
    disp(['An error occurred: ' ME.message]);
end
