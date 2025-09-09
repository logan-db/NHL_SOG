#!/bin/bash

# Script to update removal date and deploy the bundle
# This ensures the removal date is always current before deployment

set -e  # Exit on any error

echo "🚀 Deploying NHL SOG Bundle with Dynamic Removal Date"
echo "=================================================="

# Calculate date 2 months from today
REMOVAL_DATE=$(date -v+2m +%Y-%m-%d)
echo "📅 Setting removal date to: $REMOVAL_DATE"

# Update the databricks.yml file with the new date
echo "📝 Updating databricks.yml with new removal date..."
sed -i.bak "s/default: \"[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}\"/default: \"$REMOVAL_DATE\"/" databricks.yml

# Validate the configuration
echo "✅ Validating bundle configuration..."
databricks bundle validate --profile dev

# Deploy the bundle
echo "🚀 Deploying bundle..."
databricks bundle deploy --profile dev

echo "✅ Deployment complete!"
echo "📅 Resources marked for removal on: $REMOVAL_DATE"
echo "💡 To update the removal date in the future, just run this script again"
