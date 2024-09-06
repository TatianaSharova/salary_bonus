mkdir $HOME/secrets


gpg --quiet --batch --yes --decrypt --passphrase="$CREDS_JSON" \
--output $HOME/secrets/creds.json creds.json.gpg