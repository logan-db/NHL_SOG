#!/bin/bash

# Script to update the removal date to today + 2 months
# This ensures the removal date is always current

# Calculate date 2 months from today
REMOVAL_DATE=$(date -v+2m +%Y-%m-%d)

echo "Updating removal date to: $REMOVAL_DATE"

# Update the databricks.yml file with the new date
sed -i.bak "s/default: \"[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}\"/default: \"$REMOVAL_DATE\"/" databricks.yml

echo "Removal date updated to: $REMOVAL_DATE"
echo "You can now deploy with: databricks bundle deploy --profile dev"
