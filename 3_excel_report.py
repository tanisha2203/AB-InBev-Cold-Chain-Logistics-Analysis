import pandas as pd

# 1. READ THE ANALYZED DATA
input_filename = 'shipment_analysis_output.csv'
try:
    df = pd.read_csv(input_filename)
    print(f"Reading analyzed data from '{input_filename}'...")
except FileNotFoundError:
    print(f"Error: The file '{input_filename}' was not found.")
    print("Please ensure you have successfully run the '2_analysis.py' script first.")
    exit()

# 2. PERFORM THE TACTICAL ANALYSIS
shipments_summary = df.drop_duplicates(subset='ShipmentID').copy()

spoilage_by_route = shipments_summary.groupby('RouteName')['IsSpoiled'].mean().reset_index()
spoilage_by_route.rename(columns={'IsSpoiled': 'SpoilageRate'}, inplace=True)

top_10_risky_routes = spoilage_by_route.sort_values(by='SpoilageRate', ascending=False).head(10)

# 3. FORMAT THE REPORT
top_10_risky_routes['SpoilageRate_%'] = (top_10_risky_routes['SpoilageRate'] * 100)

# Select and rename columns for the final report.
# Adding .copy() here to fix the SettingWithCopyWarning.
final_report = top_10_risky_routes[['RouteName', 'SpoilageRate_%']].copy()
final_report.rename(columns={'RouteName': 'High-Risk Route', 'SpoilageRate_%': 'Spoilage Rate (%)'}, inplace=True)

# 4. SAVE THE REPORT TO AN EXCEL FILE
output_excel_filename = 'High_Risk_Routes_Action_Report.xlsx'

# Using 'xlsxwriter' as the engine, as it supports the .add_format() method.
with pd.ExcelWriter(output_excel_filename, engine='xlsxwriter') as writer:
    final_report.to_excel(writer, index=False, sheet_name='Risky Routes Report')
    
    # Get the workbook and worksheet objects from the xlsxwriter engine
    workbook  = writer.book
    worksheet = writer.sheets['Risky Routes Report']
    
    # Add a percentage format to the 'Spoilage Rate (%)' column
    percent_format = workbook.add_format({'num_format': '0.00"%"'})
    worksheet.set_column('B:B', 18, percent_format) # Column B, width 18
    worksheet.set_column('A:A', 30) # Column A, width 30

print(f"\n✅ Phase 3 Complete! Actionable report saved to '{output_excel_filename}'")