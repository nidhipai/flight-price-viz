import pandas as pd
import os
import os.path


def merge():
    # Import the data
    coupon_2018 = pd.read_csv(".data/Origin_and_Destination_Survey_DB1BCoupon_2018_1/Origin_and_Destination_Survey_DB1BCoupon_2018_1.csv")
    ticket_2022 = pd.read_csv(".data/Origin_and_Destination_Survey_DB1BTicket_2022_1/Origin_and_Destination_Survey_DB1BTicket_2022_1.csv")
    ticket_2018 = pd.read_csv(".data/Origin_and_Destination_Survey_DB1BTicket_2018_1/Origin_and_Destination_Survey_DB1BTicket_2018_1.csv")
    coupon_2022 = pd.read_csv(".data/Origin_and_Destination_Survey_DB1BCoupon_2022_1/Origin_and_Destination_Survey_DB1BCoupon_2022_1.csv")

    # Obtain wanted data from dataframes
    coupon_2018 = coupon_2018[['ItinID', 'Year', 'Quarter', 'OriginAirportID', 'Origin', 'DestAirportID', 'Dest']]
    coupon_2022 = coupon_2022[['ItinID', 'Year', 'Quarter', 'OriginAirportID', 'Origin', 'DestAirportID', 'Dest']]

    ticket_2018 = ticket_2018[
        ['ItinID', 'Year', 'Quarter', 'Origin', 'OriginAirportID', 'RoundTrip', 'OnLine',
         'RPCarrier', 'ItinFare']]
    ticket_2022 = ticket_2022[
        ['ItinID', 'Year', 'Quarter', 'Origin', 'OriginAirportID', 'RoundTrip', 'OnLine',
         'RPCarrier', 'ItinFare']]
    # Concatenate dataframes
    ticket_total = pd.concat([ticket_2018, ticket_2022], ignore_index=True)
    coupon_total = pd.concat([coupon_2018, coupon_2022], ignore_index=True)

    # Merge to obtain one dataframe
    df_total = pd.merge(ticket_total, coupon_total, left_on=['ItinID', 'Year', 'Quarter', 'Origin', 'OriginAirportID'],
                        right_on=['ItinID', 'Year', 'Quarter', 'Origin', 'OriginAirportID'])

    # Make file for total csv
    os.makedirs('.data/combined', exist_ok=True)
    df_total.to_csv('.data/combined/df_total.csv')

def cleaner(airline_name):
    if os.path.exists('.data/combined/df_total.csv') == False:
        merge()
    total = pd.read_csv('.data/combined/df_total.csv')
    company_names = {'9E': 'Delta', 'AA': 'American', 'AS': 'Alaska', 'B6': 'Jet Blue', 'CP': 'Compass',
                        'DL': 'Delta', 'EV': 'ExpressJet', 'F9': 'Frontier', 'G4': 'Allegiant', 'G7': 'United',
                        'HA': 'Hawaiian', 'MQ': 'American', 'NK': 'Spirit', 'OH': 'American', 'OO': 'Misc.',
                        'QX': 'Alaska', 'SY': 'Sun Country', 'UA': 'United', 'VX': 'Misc.', 'WN': 'Southwest',
                        'YV': 'Misc.', 'YX': 'Misc.', 'ZW': 'United', 'C5': 'United', 'PT': 'American', '3M': 'Misc.',
                        'MX': 'Breeze', 'XP':'Avelo'}

    # Replace carrier codes with wanted names
    total = total.replace({"RPCarrier": company_names})

    # Average over wanted variables
    averaged = total.groupby(['Year', 'Quarter', 'Origin', 'OriginAirportID', 'DestAirportID', 'Dest', 'RPCarrier'])['ItinFare'].mean().reset_index()

    # Filter by carrier code
    averaged = averaged.loc[averaged['RPCarrier'] == airline_name]

    #TO DO: Figure out percent change month over month ######## ####### ####### ######


    #averaged['pct_change'] = averaged.groupby(['OriginAirportID','DestAirportID','Year'])['ItinFare'].pct_change()
    #averaged = averaged[averaged['pct_change'] != 0]
    #averaged = averaged[averaged['pct_change'].notna()]
    #print(averaged.groupby(['Year', 'Quarter', 'Origin', 'OriginAirportID', 'DestAirportID', 'Dest', 'RPCarrier']))
    print(averaged.sort_values(by=['OriginAirportID','DestAirportID','Year']))
    #print(averaged['Year'].unique())
    #print(averaged.head())

cleaner('Delta')