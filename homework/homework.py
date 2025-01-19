"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    import zipfile
    import pandas as pd
    import os
    import numpy as np


    # Función para extraer archivos CSV de un archivo ZIP y devolver un DataFrame
    def extract_csv_from_zip(zip_file):
        with zipfile.ZipFile(zip_file, 'r') as z:
            # Lista de archivos CSV en el ZIP
            csv_files = [f for f in z.namelist() if f.endswith('.csv')]
            
            # Lista para almacenar DataFrames
            df_list = []
            
            # Extraer cada archivo CSV y leerlo en un DataFrame
            for csv_file in csv_files:
                with z.open(csv_file) as f:
                    df = pd.read_csv(f)
                    df_list.append(df)
                    
        # Concatenar todos los DataFrames en uno solo
        combined_df = pd.concat(df_list, ignore_index=True)
        return combined_df

    # Lista de archivos ZIP
    zip_files = os.listdir(r"D:\Unal\Analitica Descriptiva\Labs\2024-2-LAB-05-limpieza-de-datos-de-campanas-de-marketing-DanielOrozco09\files\input")

    # Lista para almacenar DataFrames de cada archivo ZIP
    all_dfs = []

    # Extraer archivos CSV de cada archivo ZIP y añadir a la lista
    for zip_file in zip_files:
        df = extract_csv_from_zip(rf"D:\Unal\Analitica Descriptiva\Labs\2024-2-LAB-05-limpieza-de-datos-de-campanas-de-marketing-DanielOrozco09\files\input\{zip_file}")
        all_dfs.append(df)

    # Combinar todos los DataFrames en uno solo
    final_df = pd.concat(all_dfs, ignore_index=True)


    df = final_df

    df = df.rename(columns = {'mortgage' : 'mortage'})
    # Crear el directorio de salida si no existe
    output_dir = r'D:\Unal\Analitica Descriptiva\Labs\2024-2-LAB-05-limpieza-de-datos-de-campanas-de-marketing-DanielOrozco09\files\output'
    os.makedirs(output_dir, exist_ok=True)

    # Procesar client.csv
    client_df = df[['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortage']].copy()
    client_df['job'] = client_df['job'].str.replace('.', '').str.replace('-', '_')
    client_df['education'] = client_df['education'].str.replace('.', '_').replace('unknown', pd.NA)
    client_df['credit_default'] = client_df['credit_default'].apply(lambda x: 1 if x == 'yes' else 0)
    client_df['mortage'] = client_df['mortage'].apply(lambda x: 1 if x == 'yes' else 0)
    client_df = client_df.rename(columns = {'mortage' : 'mortgage'})
    client_df.to_csv(os.path.join(output_dir, 'client.csv'), index=False)

    # Procesar campaign.csv
    campaign_df = df[['client_id', 'number_contacts', 'contact_duration', 'previous_campaign_contacts', 'previous_outcome', 'campaign_outcome', 'day', 'month']].copy()
    campaign_df['previous_outcome'] = campaign_df['previous_outcome'].apply(lambda x: 1 if x == 'success' else 0)
    campaign_df['campaign_outcome'] = campaign_df['campaign_outcome'].apply(lambda x: 1 if x == 'yes' else 0)
    campaign_df['last_contact_date'] = pd.to_datetime(campaign_df['day'].astype(str) + '-' + campaign_df['month'] + '-2022', format='%d-%b-%Y')
    campaign_df.drop(columns=['day', 'month'], inplace=True)
    campaign_df.to_csv(os.path.join(output_dir, 'campaign.csv'), index=False)

    # Procesar economics.csv
    economics_df = df[['client_id', 'cons_price_idx', 'euribor_three_months']].copy()
    economics_df.to_csv(os.path.join(output_dir, 'economics.csv'), index=False)

    print("Los archivos se han guardado exitosamente en el directorio files/output/.")
    
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

    return


if __name__ == "__main__":
    clean_campaign_data()
