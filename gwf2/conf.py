import os
import ConfigParser

CONFIG_FILE = os.getenv('GWF_CONFIG_FILE', os.path.expanduser('~/.gwf2rc'))
parser = ConfigParser.RawConfigParser()

if not os.path.isfile(CONFIG_FILE):
    # Create config with defaults
    parser.add_section('Main')
    parser.set('Main', 'scheduler', 'PBS')
    parser.set('Main', 'scheduler_options', 'PBS | Other')

    # Write file
    with open(CONFIG_FILE, 'wb') as configfile:
        parser.write(configfile)

else:
    parser.read(CONFIG_FILE)
    

# Check configuration
CHECK = {'scheduler': ['PBS']}

for val in CHECK.keys():    
    fetch = parser.get('Main', val)
    if fetch not in CHECK[val]:
        raise Exception('Configuration issue for \''+val+' = '+fetch+'\'. \''+fetch+'\' not in list '+str(CHECK[val]) + '. Fix '+CONFIG_FILE+'!')

# Provide function to retrieve configuration value
def get(val):
    return parser.get('Main', val)
