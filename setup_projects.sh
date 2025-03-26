#!/bin/bash

ROOT_DIR=~/dev/mapper-csv
DJ_DIR=../dj

# Parallel arrays
PROJECT_NAMES=("senzing_test_data" "global_legal_firms")
EXCEL_FILES=("Senzing Test Data.xlsx" "Global Legal Firms.xlsx")

cd "$ROOT_DIR" || exit

echo "🔧 Setting up project folders and conversions..."

# Install csvkit if needed
if ! command -v in2csv &> /dev/null; then
  echo "Installing csvkit..."
  pip3 install --quiet csvkit
fi

for i in "${!PROJECT_NAMES[@]}"; do
  PROJECT="${PROJECT_NAMES[$i]}"
  XL_FILE="${EXCEL_FILES[$i]}"
  XL_SRC_PATH="$DJ_DIR/$XL_FILE"
  XL_DEST_PATH="$ROOT_DIR/projects/$PROJECT/input/${XL_FILE}"
  CSV_DEST_PATH="$ROOT_DIR/projects/$PROJECT/input/${PROJECT}.csv"

  echo "📁 Creating structure for $PROJECT..."
  mkdir -p projects/$PROJECT/{input,mappings,output}

  echo "📄 Copying mapping tools into $PROJECT..."
  cp csv_analyzer.py csv_mapper.py csv_functions.py csv_functions.json python_template.py projects/$PROJECT/

  echo "🗂️  Copying Excel file to input/..."
  cp "$XL_SRC_PATH" "$XL_DEST_PATH"

  echo "🔄 Converting $XL_FILE to CSV..."
  in2csv "$XL_SRC_PATH" > "$CSV_DEST_PATH"

  echo "⚙️  Creating convert.sh for custom pre-processing..."
  cat <<EOF > projects/$PROJECT/convert.sh
#!/bin/bash
# Optional: Place your custom conversion or cleanup logic here
echo "Currently no extra processing needed for $PROJECT"
EOF
  chmod +x projects/$PROJECT/convert.sh

  echo "✅ Project $PROJECT setup complete."
  echo
done

echo "🎉 All projects scaffolded successfully."