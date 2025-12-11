import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import yfinance as yf 
import psycopg2
from psycopg2 import OperationalError
import random

# --- 1. CONFIGURACIÓN DE LA BASE DE DATOS ---
DB_CONFIG = {
    'host': 'proyecto-henry-db.cc34ckg64vg0.us-east-1.rds.amazonaws.com',
    'database': 'postgres',
    'user': 'sp500',
    'password': 'henrySP500',
    'port': '5432'
}

# --- DEFINICIÓN DE NOMBRES DE COLUMNAS ---
# CLAVE DE PYTHON (USADA EN EL CÓDIGO) : NOMBRE REAL EN LA BASE DE DATOS (VISTA)
COL_MAP = {
    # Vistas Top 5 (Dashboard General)
    'action_name': 'symbol',              
    'industry_name_general': 'industry',  
    'sector_name_general': 'sector',      
    'action_return': 'annual_return',     # Clave Python: action_return. Columna DB: annual_return
    
    # Vistas por Compañía (Dashboard por Acción)
    'symbol_key': 'symbol',               
    'company_name_key': 'company_name',   
    
    # v_company_overview
    'sector_name_overview': 'sector_name', 
    'industry_name_overview': 'industry_name',
    'employees_key': 'employees', 
    'description_key': 'company_description',
    
    # v_company_reviews
    'review_score_key': 'rating', 
    
    # v_company_risk
    'risk_score_key': 'esg_risk_score', 
    
    # v_company_value_year 
    'year_high_key': 'year_high', 
    'year_low_key': 'year_low', 
    'annual_return_value_key': 'annual_return' # Columna DB: annual_return. Clave Python: annual_return_value_key
}
# --------------------------------------------------------------------------

# --- 2. FUNCIONES DE CONEXIÓN Y DATOS ---

@st.cache_resource(ttl=3600) 
def get_db_connection():
    try:
        # Añadir 'as Any' para satisfacer a Pylance sobre la sobrecarga de connect
        conn = psycopg2.connect(**DB_CONFIG) # type: ignore 
        return conn
    except OperationalError as e:
        st.error(f"Error de conexión a la base de datos. Verifique DB_CONFIG. Error: {e}")
        return None

@st.cache_data(ttl=3600) 
def fetch_data_from_db(table_name):
    conn = get_db_connection()
    if conn:
        try:
            query = f"SELECT * FROM {table_name};"
            df = pd.read_sql(query, conn)
            
            # PASO 1: Normalizar los nombres de columnas originales del DataFrame 
            df.columns = df.columns.str.lower().str.strip().str.replace(' ', '_')
            
            # PASO 2: Renombrar columnas de la DB a las claves de Python (Mapeo 1:1).
            rename_map = {}
            
            # Columnas de DB que tienen mapeo doble y se manejan después
            DOUBLE_MAPPED_COLS = ['annual_return', 'symbol'] 
            
            # Iteramos sobre el COL_MAP (Clave Python : Nombre de DB)
            for py_key, db_col in COL_MAP.items():
                normalized_db_col = db_col.lower().strip().replace(' ', '_')
                
                # Buscamos la columna de la DB normalizada en el DataFrame cargado
                if normalized_db_col in df.columns:
                    
                    # 1. Si la clave de Python (el nuevo nombre) ya está en el DF, lo ignoramos.
                    if py_key in df.columns:
                         continue
                         
                    # 2. Si es una columna de doble mapeo, la saltamos para el renombramiento 1:1.
                    if normalized_db_col in DOUBLE_MAPPED_COLS:
                         continue 

                    # 3. Para todas las demás columnas (mapeo 1:1), las agregamos al mapa.
                    rename_map[normalized_db_col] = py_key

            # Aplicar el renombrado 1:1
            if rename_map:
                df.rename(columns=rename_map, inplace=True)
            
            # PASO 3: Manejo explícito de las columnas de doble mapeo (CRÍTICO)

            # A. Manejo de 'annual_return'
            if 'annual_return' in df.columns:
                # Aseguramos 'action_return' (Top 5)
                if 'action_return' not in df.columns:
                    df['action_return'] = df['annual_return']
                # Aseguramos 'annual_return_value_key' (Dashboard por Acción)
                if 'annual_return_value_key' not in df.columns:
                    df['annual_return_value_key'] = df['annual_return']
                    
            # B. Manejo de 'symbol'
            if 'symbol' in df.columns:
                # Aseguramos 'action_name' (Top 5 - GRÁFICO GENERAL)
                if 'action_name' not in df.columns:
                    df['action_name'] = df['symbol']
                # Aseguramos 'symbol_key' (Búsqueda por Ticker)
                if 'symbol_key' not in df.columns:
                    df['symbol_key'] = df['symbol']

            # PASO 4: Asegurar 'company_name_key' si 'company_name' existe.
            if 'company_name' in df.columns and 'company_name_key' not in df.columns:
                df['company_name_key'] = df['company_name']
            
            
            # PASO 5: Estandarizar las columnas clave de Python (para asegurar búsquedas consistentes)
            if 'symbol_key' in df.columns: 
                 df['symbol_key'] = df['symbol_key'].astype(str).str.strip().str.upper() 
            if 'company_name_key' in df.columns:
                 df['company_name_key'] = df['company_name_key'].astype(str).str.strip().str.lower()
            if 'action_name' in df.columns:
                 df['action_name'] = df['action_name'].astype(str).str.strip().str.upper() 

            return df
        except Exception as e:
            st.error(f"Error CRÍTICO al obtener datos de {table_name}. Error: {e}")
            return pd.DataFrame()
    st.error("Error: Conexión a DB no disponible.")
    return pd.DataFrame()

# --- FUNCIONES DE VISTAS DEL DASHBOARD (yfinance para 500 empresas) ---

@st.cache_data(ttl=3600)
def load_sp500_tickers():
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        
        sp500_tickers = pd.read_html(
            'https://en.wikipedia.org/wiki/List_of_S%26P%20500_companies', 
            storage_options={'User-Agent': headers['User-Agent']}
        )[0]
        
        tickers = [t.strip() for t in sp500_tickers['Symbol'].tolist()]
        bad_tickers = ['BRK.B', 'BF.B'] 
        tickers = [t for t in tickers if t not in bad_tickers]
        
        download_list = ['^GSPC'] + tickers 
        download_result = yf.download(download_list, start='2020-01-01', progress=False)
        
        if download_result is None or download_result.empty:
            raise Exception("Datos de yfinance vacíos o incompletos.")
            
        data = pd.DataFrame(download_result) 

        close_data = pd.DataFrame() # Inicializar como DataFrame vacío por defecto
        
        if isinstance(data.columns, pd.MultiIndex):
            # Caso 1: MultiIndex (Descarga exitosa de múltiples tickers)
            close_data = data['Close']
            
            # Lógica de filtrado OHLC (se mantiene igual, ya corregida)
            all_ohlc_cols = ['Open', 'High', 'Low', 'Close']
            valid_ohlc_tickers = []
            for ticker in data.columns.levels[1]:
                # Comprobamos que el ticker tenga las 4 columnas
                cols_check = [(col, ticker) for col in all_ohlc_cols]
                # Verificamos si todas las tuplas de columna están presentes
                if all(col in data.columns for col in cols_check):
                    valid_ohlc_tickers.append(ticker)
            
            final_tickers_for_ohlc = [t for t in valid_ohlc_tickers if t in data.columns.levels[1]]
            
            cols_to_keep = [col for col in data.columns if col[1] in final_tickers_for_ohlc]
            ohlc_data_filtered = data[cols_to_keep].copy()

            st.session_state.yfinance_ohlc = ohlc_data_filtered 

        else:
            # Caso 2: Sin MultiIndex (Suele ocurrir si solo se descargó un ticker, ej. solo ^GSPC)
            
            # Si 'Close' está presente, la usamos. Si no, usamos todo el DF (aunque es poco probable)
            if 'Close' in data.columns:
                 close_data = data[['Close']].copy() # Forzamos a ser un DataFrame de una columna
            else:
                 close_data = data.copy()
                 
            st.session_state.yfinance_ohlc = pd.DataFrame() # No hay OHLC completo para RT Analysis
        
        
        # MANEJO DEL OBJETO close_data (Corrección de Series/DataFrame)
        if isinstance(close_data, pd.Series):
             # Si es una Series, la convertimos en un DataFrame con el nombre de la Series como columna
             close_data = close_data.to_frame()
             
        if not close_data.empty:
             # Solo renombramos si la columna existe en el DataFrame
             if '^GSPC' in close_data.columns:
                 close_data.rename(columns={'^GSPC': 'S&P 500'}, inplace=True) # type: ignore 
        else:
             st.warning("El resultado de yfinance es un DataFrame/Series vacío.")
             return tickers, pd.DataFrame() 

        # Solo devolvemos los tickers para los cuales tenemos datos de cierre
        valid_tickers = [t for t in tickers if t in close_data.columns]
        
        return valid_tickers, close_data.dropna()
        
    except Exception as e:
        # Fallback (Manejo de errores si yfinance falla)
        st.warning(f"Error al obtener tickers o datos históricos. Usando fallback. ({e})")
        tickers = ['AAPL', 'MSFT', 'JPM', 'XOM', 'AMZN']
        dates = pd.date_range(start='2020-01-01', periods=1000, freq="B")
        sim_data = pd.DataFrame(index=dates)
        sim_data['S&P 500'] = 1000 + np.cumsum(np.random.randn(1000) * 10)
        for ticker in tickers:
             sim_data[ticker] = sim_data['S&P 500'] * np.random.uniform(0.8, 1.2, 1000)
        
        ohlc_cols = ['Close', 'High', 'Low', 'Open', 'Volume', 'Adj Close']
        multi_index_cols = pd.MultiIndex.from_product([ohlc_cols, ['S&P 500'] + tickers])
        
        data_sim = pd.DataFrame(index=dates, columns=multi_index_cols)
        for ticker in ['S&P 500'] + tickers:
            data_sim[('Close', ticker)] = sim_data[ticker]
            data_sim[('Open', ticker)] = sim_data[ticker] * 0.99
            data_sim[('High', ticker)] = sim_data[ticker] * 1.01
            data_sim[('Low', ticker)] = sim_data[ticker] * 0.98
            data_sim[('Adj Close', ticker)] = sim_data[ticker]
            data_sim[('Volume', ticker)] = 1000000 
        
        st.session_state.yfinance_ohlc = data_sim
        
    return tickers, sim_data.dropna()
STOCK_TICKERS, df_hist = load_sp500_tickers()

# ... (El resto del código de utilidades, safe_float_conversion, calculate_historical_trend, etc.)

def safe_float_conversion(value):
    if value is None:
        return None
    try:
        str_value = str(value).strip().replace(',', '').replace('%', '')
        if str_value.lower() in ('nan', 'n/a', 'none', ''):
             return None
        return float(str_value)
    except (ValueError, TypeError):
        return None

def calculate_historical_trend(ticker, df_hist):
    if ticker not in df_hist.columns:
        return None, "No hay datos históricos."
    recent_prices = df_hist[ticker].tail(60)
    if recent_prices.empty or len(recent_prices) < 2:
        return None, "Datos recientes insuficientes."
    start_price = recent_prices.iloc[0]
    end_price = recent_prices.iloc[-1]
    if start_price == 0:
        return None, "Precio inicial es cero."
    trend = (end_price / start_price - 1) * 100
    
    if trend >= 5.0:
        return "ALCISTA", f"Aumento del {trend:.2f}% en los últimos 3 meses."
    elif trend <= -5.0:
        return "BAJISTA", f"Caída del {trend:.2f}% en los últimos 3 meses."
    else:
        return "NEUTRA", f"Cambio del {trend:.2f}% (sin tendencia clara)."

def get_recommendation(ticker, df_hist, annual_return, esg_score, rating):
    """Genera la recomendación basada en DB y Tendencia histórica."""
    trend_status, trend_advice = calculate_historical_trend(ticker, df_hist)
    annual_ret = safe_float_conversion(annual_return)
    esg_score = safe_float_conversion(esg_score)
    rating = safe_float_conversion(rating)

    if trend_status is None or trend_status == "NEUTRA":
        if annual_ret is not None and annual_ret > 10 and (esg_score is None or esg_score < 20):
            return "COMPRA", "green", "Tendencia neutra, pero el rendimiento anual es positivo y el riesgo es bajo/desconocido."
        elif annual_ret is not None and annual_ret < 0 or (esg_score is not None and esg_score > 35):
            return "EVITAR", "red", "Rendimiento negativo o el riesgo ESG es muy alto."
        return "NEUTRA", "black", f"Datos fundamentales insuficientes. {trend_advice}"

    if trend_status == "ALCISTA":
        if annual_ret is not None and annual_ret > 20 and (esg_score is None or esg_score < 30):
            return "COMPRA FUERTE", "green", f"Tendencia alcista sostenida y fundamentales sólidos. {trend_advice}"
        else:
            return "MANTENER", "orange", f"Tendencia alcista reciente, pero los datos fundamentales son moderados. {trend_advice}"

    elif trend_status == "BAJISTA":
        if esg_score is not None and esg_score > 30 and (annual_ret is None or annual_ret < 0):
            return "VENTA / EVITAR", "red", f"Tendencia bajista clara, alto riesgo ESG o bajo rendimiento. {trend_advice}"
        else:
            return "MANTENER", "orange", f"Tendencia bajista, pero las métricas fundamentales no son críticas. {trend_advice}"

    return "N/A", "black", "Error en la evaluación del semáforo."

def get_combined_recommendation(ticker, df_hist, annual_return, esg_score, rating, sentiment_positive_pct):
    """Genera la recomendación combinada (DB + Tendencia + Sentimiento IA)."""
    
    initial_recommendation, color, advice = get_recommendation(ticker, df_hist, annual_return, esg_score, rating)
    
    sentiment_boost = sentiment_positive_pct - 50 

    if sentiment_positive_pct >= 75:
        if initial_recommendation in ["COMPRA", "MANTENER"]:
            return "COMPRA FUERTE", "green", f"¡Alerta de Noticias! Sentimiento muy positivo ({sentiment_positive_pct:.1f}%). {advice}"
        elif initial_recommendation == "NEUTRA":
            return "COMPRA", "green", f"Sentimiento positivo elevado. {advice}"
    
    elif sentiment_positive_pct <= 25:
        if initial_recommendation in ["VENTA / EVITAR", "MANTENER"]:
            return "VENTA / EVITAR", "red", f"¡Alerta de Noticias! Sentimiento muy negativo ({sentiment_positive_pct:.1f}%). {advice}"
        elif initial_recommendation == "COMPRA FUERTE":
            return "MANTENER", "orange", f"Riesgo de Noticias. Sentimiento negativo, mitiga la compra. {advice}"
            
    elif sentiment_boost > 10: 
         if initial_recommendation == "MANTENER":
             return "COMPRA MODERADA", "green", f"El sentimiento positivo apoya una compra moderada. {advice}"
         
    elif sentiment_boost < -10:
         if initial_recommendation == "COMPRA FUERTE":
             return "MANTENER", "orange", f"El sentimiento negativo aconseja cautela. {advice}"

    return initial_recommendation, color, advice 

def get_sentiment_data_simulated(ticker):
    random.seed(hash(ticker) % 100)
    
    positive = random.uniform(30.0, 75.0)
    negative = random.uniform(10.0, 30.0)
    neutral = 100.0 - positive - negative
    
    total = positive + negative + neutral
    positive = (positive / total) * 100
    negative = (negative / total) * 100
    neutral = (neutral / total) * 100

    titles = [
        f'Grandes expectativas de crecimiento para {ticker} impulsan las acciones.',
        f'Analistas advierten sobre la posible sobrevaloración de {ticker}.',
        f'El sector tecnológico impulsa las ganancias de {ticker} este trimestre.',
        f'Rumores de cambio de liderazgo generan incertidumbre en {ticker}.',
        f'{ticker} anuncia expansión y nueva inversión.',
    ]
    sentiment_list = [
        {'title': titles[0], 'score': random.uniform(0.7, 0.95), 'label': 'Positivo'},
        {'title': titles[1], 'score': random.uniform(-0.6, -0.9), 'label': 'Negativo'},
        {'title': titles[2], 'score': random.uniform(0.5, 0.8), 'label': 'Positivo'},
        {'title': titles[3], 'score': random.uniform(-0.3, -0.6), 'label': 'Negativo'},
        {'title': titles[4], 'score': random.uniform(0.6, 0.9), 'label': 'Positivo'},
    ]
    
    return {
        'positive_pct': positive,
        'negative_pct': negative,
        'neutral_pct': neutral,
        'news_list': sentiment_list
    }
    
def get_ticker_data(df, filter_value, filter_col):
    """
    Busca datos por el valor de filtro (symbol o company_name) en la columna especificada.
    filter_col debe ser la CLAVE de Python (ej. 'symbol_key', 'company_name_key').
    """
    if df.empty or filter_col not in df.columns:
        return {}

    filter_value_sane = str(filter_value).strip()
    
    # Estandarización del valor de búsqueda
    if filter_col == 'symbol_key':
        filter_value_sane = filter_value_sane.upper() 
    elif filter_col == 'company_name_key':
        filter_value_sane = filter_value_sane.lower() 
    
    df_search_col = df[filter_col].copy()
    
    if filter_col == 'symbol_key':
         df_search_col = df_search_col.astype(str).str.strip().str.upper()
    elif filter_col == 'company_name_key':
         df_search_col = df_search_col.astype(str).str.strip().str.lower()
    else:
         df_search_col = df_search_col.astype(str).str.strip()
    
    filtered_df = df[df_search_col == filter_value_sane]
    
    if not filtered_df.empty:
         return filtered_df.iloc[0].to_dict()
    return {}

@st.cache_data(ttl=3600)
def get_valid_tickers_with_full_data(df_overview, df_reviews, df_risk, df_value_year, all_tickers):
    """
    Filtra la lista de tickers para incluir solo aquellos con:
    1. Datos fundamentales clave (DB) completos.
    2. Datos OHLC completos (yfinance) si están en la sesión (para Real Time Analysis).
    """
    
    if df_overview.empty or df_risk.empty or df_value_year.empty or df_reviews.empty:
         return []
         
    # Obtener datos OHLC filtrados de la sesión
    ohlc_data = st.session_state.get('yfinance_ohlc', pd.DataFrame())
    
    try:
        tickers_overview = set(df_overview['symbol_key'].unique())
        company_map = df_overview[['symbol_key', 'company_name_key']].set_index('symbol_key')['company_name_key'].to_dict()
    except KeyError as e:
        st.error(f"Error al acceder a las columnas clave (symbol_key/company_name_key) en los DataFrames de Overview. Clave faltante: {e}")
        return []
        
    final_valid_list = []
    
    ohlc_cols_needed = ['Open', 'High', 'Low', 'Close']
    
    for ticker in tickers_overview:
        # CRÍTICO: Asegurar que el ticker esté en la lista general de tickers y se haya podido descargar
        if ticker not in all_tickers: 
             continue

        company_name_lower = company_map.get(ticker)
        if not company_name_lower:
            continue
            
        # --- 1. Verificación de Datos Fundamentales (DB) ---
        # Usamos la búsqueda por la clave de Python ya limpia
        value_data = get_ticker_data(df_value_year, ticker, filter_col='symbol_key')
        risk_data = get_ticker_data(df_risk, ticker, filter_col='symbol_key')
        review_data = get_ticker_data(df_reviews, company_name_lower, filter_col='company_name_key') 
        
        annual_ret = value_data.get('annual_return_value_key')
        esg_score = risk_data.get('risk_score_key')
        rating = review_data.get('review_score_key') 
        
        db_data_complete = (
            safe_float_conversion(annual_ret) is not None and 
            safe_float_conversion(esg_score) is not None and
            safe_float_conversion(rating) is not None
        )
        
        if not db_data_complete:
            continue # Saltar si los datos de la DB no están completos
            
        # --- 2. Verificación de Datos OHLC (yfinance) ---
        ohlc_complete = False
        if not ohlc_data.empty and isinstance(ohlc_data.columns, pd.MultiIndex):
            # Comprobar si todas las columnas [('Open', ticker), ('High', ticker), etc.] existen
            cols_check = [(col, ticker) for col in ohlc_cols_needed]
            if all(col in ohlc_data.columns for col in cols_check):
                ohlc_complete = True
        
        if not ohlc_complete:
            # Si el ticker tiene datos fundamentales pero no OHLC, se excluye del análisis detallado.
            continue 

        # Si llegamos aquí, los datos de DB y OHLC están completos
        final_valid_list.append(ticker)
                     
    return sorted(final_valid_list)


# -------------------------------------------------------------
# --- PÁGINAS DEL DASHBOARD ---
# -------------------------------------------------------------
# ... (El resto de las funciones de la página permanecen sin cambios)

def page_home():
    st.title("🏦 Plataforma de Análisis Integral del S&P 500")
    st.markdown("---")
    
    st.header("📊 El Índice S&P 500: El Pulso del Mercado")
    st.write("""
        El S&P 500 (Standard & Poor's 500) es un **índice bursátil** que representa el rendimiento de **500 de las empresas más grandes** que cotizan en bolsas de Estados Unidos. 
    """)
    
    st.markdown("---")
    
    st.header("✨ Estructura de Nuestros Informes")
    
    col_g, col_a = st.columns(2)
    
    with col_g:
        st.subheader("1. 📈 Informe General (Macro)")
        st.info("""
            * **Top 5 Acciones** por mejor rendimiento anual.
            * **Distribución de Rendimiento** por Industria y Sector.
            * **Gráfico Histórico** de la evolución del índice S&P 500.
        """)
        
    with col_a:
        st.subheader("2. 🔍 Informe por Acción (Detalle)")
        st.info("""
            * **Descripción** (Sector, Industria y Empleados).
            * **Review de Analistas** (Score promedio).
            * **Riesgo ESG** (Medición de factores Ambientales, Sociales y de Gobierno).
            * **Semáforo de Inversión** (Recomendación Compra/Venta).
        """)
            
    st.markdown("---")
    st.subheader("Instrucciones")
    st.write("Usa los botones de la barra lateral izquierda para acceder a cada dashboard.")


def page_general_dashboard():
    st.title("📈 Informe General del S&P 500")
    
    df_top5_actions = fetch_data_from_db('v_top5_actions')
    df_top5_industry = fetch_data_from_db('v_top5_actions_industry')
    df_top5_sector = fetch_data_from_db('v_top5_actions_sector')
    
    st.header(f"📅 Análisis de Rendimiento (Último Período)")
    
    col_top1, col_top2 = st.columns(2)
    
    ACTION_NAME = 'action_name' 
    ACTION_RETURN = 'action_return' 
    INDUSTRY_NAME = 'industry_name_general' 
    SECTOR_NAME = 'sector_name_general' 
    
    def plot_top5(df, title, x_col, y_col, plot_type):
        fig = None # Inicializar fig 
        
        if df.empty:
             return f"Datos de '{title}' vacíos (DataFrame vacío desde DB)."
        if y_col not in df.columns:
             return f"Datos de '{title}' disponibles, pero falta la columna esperada '{y_col}' (Error de renombramiento)."
        if x_col not in df.columns:
             return f"Datos de '{title}' disponibles, pero falta la columna clave '{x_col}' (Error de renombramiento)."
        
        df_plot = df.copy()
        df_plot[y_col] = df_plot[y_col].apply(safe_float_conversion)
        
        if 'year' in df_plot.columns:
             latest_year = df_plot['year'].max()
             df_plot = df_plot[df_plot['year'] == latest_year]
             title = f"{title} ({latest_year})"
             
        df_plot.dropna(subset=[y_col, x_col], inplace=True)
        
        if df_plot.empty:
             return f"No hay datos numéricos válidos en '{y_col}' después de la conversión o filtro de año para {title}."
             
        df_plot = df_plot.sort_values(by=y_col, ascending=False).head(5)
        
        x_label = x_col.replace('_general', '').replace('_name', '').capitalize()
        
        if plot_type == 'bar':
             fig = px.bar(df_plot, x=x_col, y=y_col, 
                         title=title, color=y_col, 
                         color_continuous_scale=px.colors.sequential.Viridis, 
                         labels={x_col: x_label, y_col: 'Rendimiento Anual (%)'})
        elif plot_type == 'pie':
             fig = px.pie(df_plot, 
                         names=x_col, 
                         values=y_col, 
                         title=title, 
                         labels={x_col: x_label, y_col: 'Rendimiento Anual (%)'})
        
        if fig:
            st.plotly_chart(fig, use_container_width=True)
        else:
            return f"Error interno: no se pudo generar la figura {plot_type}."
            
        return None 

    # --- GRÁFICO 1: Top 5 Acciones ---
    with col_top1:
        st.subheader("🥇 Top 5 Acciones con Mayor Crecimiento")
        error_msg = plot_top5(df_top5_actions, "Rendimiento Acumulado (Top 5 Acciones)", ACTION_NAME, ACTION_RETURN, 'bar')
        if error_msg:
             st.warning(error_msg)

    # --- GRÁFICO 2: Top 5 Industrias ---
    with col_top2:
        st.subheader("🏭 Top 5 Industrias por Rendimiento")
        error_msg = plot_top5(df_top5_industry, "Contribución al Rendimiento Sectorial (Top 5 Industrias)", INDUSTRY_NAME, ACTION_RETURN, 'pie')
        if error_msg:
             st.warning(error_msg)
            
    st.markdown("---")
    
    col_sec, col_sp = st.columns(2)
    
    # --- GRÁFICO 3: Top 5 Sectores ---
    with col_sec:
        st.subheader("🌐 Top 5 Sectores por Rendimiento")
        error_msg = plot_top5(df_top5_sector, "Rendimiento por Sector (Top 5 Sectores)", SECTOR_NAME, ACTION_RETURN, 'bar')
        if error_msg:
             st.warning(error_msg)

    # --- GRÁFICO 4: S&P 500 ---
    with col_sp:
        st.subheader("📊 Gráfico Histórico del Índice S&P 500")
        if not df_hist.empty and 'S&P 500' in df_hist.columns:
            fig_sp = go.Figure()
            fig_sp.add_trace(go.Scatter(x=df_hist.index, y=df_hist['S&P 500'], mode='lines', name='S&P 500', line=dict(color='blue')))
            fig_sp.update_layout(title='Evolución del Índice S&P 500', xaxis_title='Fecha', yaxis_title='Precio del Índice', height=400)
            st.plotly_chart(fig_sp, use_container_width=True)
        else:
            st.warning("Datos históricos del S&P 500 no disponibles.")


def page_stock_dashboard():
    
    df_overview = fetch_data_from_db('v_company_overview')
    df_reviews = fetch_data_from_db('v_company_reviews')
    df_risk = fetch_data_from_db('v_company_risk')
    df_value_year = fetch_data_from_db('v_company_value_year')

    # La lista de tickers ahora es mucho más estricta (DB + OHLC)
    VALID_TICKERS_FULL = get_valid_tickers_with_full_data(
        df_overview, df_reviews, df_risk, df_value_year, STOCK_TICKERS
    )

    if not VALID_TICKERS_FULL:
         st.error("No hay acciones con datos fundamentales y de precios completos para el análisis detallado.")
         return
         
    selected_ticker = st.selectbox("Seleccionar Acción (S&P 500)", options=VALID_TICKERS_FULL, key='stock_ticker_select')
    st.session_state.selected_ticker = selected_ticker 
    st.title(f"🔍 Informe Detallado por Acción: {selected_ticker}")
    st.markdown("---")
    
    # 1. Obtener company_name para la búsqueda en df_reviews
    overview_data = get_ticker_data(df_overview, selected_ticker, filter_col='symbol_key')
    company_name = overview_data.get('company_name_key')
    
    # 2. Obtener el resto de los datos usando el ticker o company_name
    risk_data = get_ticker_data(df_risk, selected_ticker, filter_col='symbol_key')
    value_data = get_ticker_data(df_value_year, selected_ticker, filter_col='symbol_key')
    review_data = get_ticker_data(df_reviews, company_name, filter_col='company_name_key')

    # --- 1. Resumen Fundamental y Semáforo ---
    st.subheader(f"Resumen Fundamental de {selected_ticker}")
    col_desc, col_rev, col_risk, col_semaforo = st.columns([3, 1.5, 1.5, 2])

    annual_ret = value_data.get('annual_return_value_key')
    esg_score = risk_data.get('risk_score_key')
    rating = review_data.get('review_score_key')
    
    recommendation, color, advice = get_recommendation(
        selected_ticker, df_hist, annual_ret, esg_score, rating
    )

    with col_semaforo:
        st.subheader("🚦 Recomendación")
        st.markdown(f'<div style="background-color:{color}; color:white; padding:10px; border-radius:5px; text-align:center;">'
                    f'<h4>{recommendation}</h4></div>', unsafe_allow_html=True)
        st.caption(advice)
    
    with col_desc:
        st.caption("Descripción de la Empresa")
        st.markdown(f"**Sector:** {overview_data.get('sector_name_overview', 'N/A')}")
        st.markdown(f"**Industria:** {overview_data.get('industry_name_overview', 'N/A')}")
        
        employees = overview_data.get('employees_key', 'N/A')
        st.markdown(f"**Empleados:** {employees}") 
        
        st.markdown(f"**Descripción:** {overview_data.get('description_key', 'Descripción no disponible.')}")

    with col_rev:
        st.caption("Review de Analistas")
        score = safe_float_conversion(rating) 
        score_display = f"{score:.1f}" if score is not None else 'N/A'
        
        delta_rev = None
        if score is not None:
            delta_rev = "Compra Fuerte" if score >= 4.5 else ("Mantener" if score >= 3.0 else "Venta")
        
        st.metric("Score Promedio", f"{score_display} / 5.0", delta=delta_rev, delta_color="normal")
    
    with col_risk:
        st.caption("Riesgo (ESG Score)")
        esg_score_display = safe_float_conversion(esg_score)
        
        delta_text = None
        if esg_score_display is not None:
             delta_text = "Riesgo Alto" if esg_score_display > 30 else "Riesgo Moderado"
             esg_score_display = f"{esg_score_display:.1f}"
        else:
            esg_score_display = 'N/A'
        
        st.metric("Riesgo ESG", esg_score_display, delta=delta_text, delta_color="inverse")
    
    st.markdown("---")
    
    # --- 2. Valores Máximos/Mínimos y Gráfico Anual ---
    st.subheader("Análisis de Precios")
    col_values, col_chart = st.columns([1, 2])
    
    with col_values:
        st.caption(f"Valores Máximos y Mínimos (52 Semanas)")
        
        max_price = safe_float_conversion(value_data.get('year_high_key'))
        min_price = safe_float_conversion(value_data.get('year_low_key'))
        
        max_str = f"${max_price:.2f}" if max_price is not None else "$N/A"
        min_str = f"${min_price:.2f}" if min_price is not None else "$N/A"
        
        st.metric("Máximo 52 Semanas", max_str)
        st.metric("Mínimo 52 Semanas", min_str)
        
    with col_chart:
        st.caption(f"Gráfico Anual de Precios")
        if not df_hist.empty and selected_ticker in df_hist.columns:
            df_plot = df_hist[selected_ticker].tail(252) 
            fig_stock = go.Figure()
            fig_stock.add_trace(go.Scatter(x=df_plot.index, y=df_plot.values, mode='lines', name=selected_ticker, line=dict(color='green')))
            fig_stock.update_layout(title=f'Precio en el Último Año', xaxis_title='Fecha', yaxis_title='Precio (USD)', height=300)
            st.plotly_chart(fig_stock, use_container_width=True)
        else:
            st.warning(f"Datos históricos para {selected_ticker} no disponibles.")


def page_real_time_analysis():
    
    df_overview = fetch_data_from_db('v_company_overview')
    df_reviews = fetch_data_from_db('v_company_reviews')
    df_risk = fetch_data_from_db('v_company_risk')
    df_value_year = fetch_data_from_db('v_company_value_year')

    # La lista de tickers ahora es mucho más estricta (DB + OHLC)
    VALID_TICKERS_FULL = get_valid_tickers_with_full_data(
        df_overview, df_reviews, df_risk, df_value_year, STOCK_TICKERS
    )

    if not VALID_TICKERS_FULL:
         st.error("No hay acciones con datos fundamentales y de precios completos para el análisis en tiempo real.")
         return
         
    selected_ticker = st.selectbox("Seleccionar Acción (S&P 500)", options=VALID_TICKERS_FULL, key='rt_ticker_select')
    st.title(f"⚡ Análisis en Tiempo Real: {selected_ticker}")
    st.markdown("---")
    
    # 1. Obtener company_name para la búsqueda en df_reviews
    overview_data = get_ticker_data(df_overview, selected_ticker, filter_col='symbol_key')
    company_name = overview_data.get('company_name_key')
    
    # 2. Obtener el resto de los datos
    risk_data = get_ticker_data(df_risk, selected_ticker, filter_col='symbol_key')
    value_data = get_ticker_data(df_value_year, selected_ticker, filter_col='symbol_key')
    review_data = get_ticker_data(df_reviews, company_name, filter_col='company_name_key')

    # Obtener valores
    annual_ret = value_data.get('annual_return_value_key')
    esg_score = risk_data.get('risk_score_key')
    rating = review_data.get('review_score_key')
    
    sentiment_data = get_sentiment_data_simulated(selected_ticker)
    positive = sentiment_data['positive_pct']
    
    recommendation, color, advice = get_combined_recommendation(
        selected_ticker, df_hist, annual_ret, esg_score, rating, positive
    )
    
    # --- 2. Fila Superior: Semáforo y Métricas Fundamentales ---
    st.subheader("Evaluación Fundamental y de Sentimiento")
    col_semaforo, col_ret, col_risk = st.columns([2, 1.5, 1.5])

    with col_semaforo:
        st.subheader("🚦 Recomendación Final")
        st.markdown(f'<div style="background-color:{color}; color:white; padding:10px; border-radius:5px; text-align:center;">'
                    f'<h4>{recommendation}</h4></div>', unsafe_allow_html=True)
        st.caption(f"**(Combinada con IA):** {advice}")

    with col_ret:
        st.caption("Rendimiento Anual")
        annual_ret_disp = safe_float_conversion(annual_ret)
        annual_ret_str = f"{annual_ret_disp:.2f}%" if annual_ret_disp is not None else 'N/A'
        delta_ret = "Crecimiento" if annual_ret_disp is not None and annual_ret_disp > 0 else ("Caída" if annual_ret_disp is not None and annual_ret_disp < 0 else None)
        st.metric("Rendimiento", annual_ret_str, delta=delta_ret)

    with col_risk:
        st.caption("Riesgo (ESG Score)")
        esg_score_display = safe_float_conversion(esg_score)
        delta_text = None
        if esg_score_display is not None:
             delta_text = "Riesgo Alto" if esg_score_display > 30 else "Riesgo Moderado"
             esg_score_display = f"{esg_score_display:.1f}"
        else:
            esg_score_display = 'N/A'
        st.metric("Riesgo ESG", esg_score_display, delta=delta_text, delta_color="inverse")

    st.markdown("---")
    
    # --- 3. Fila Inferior: Gráfico de Velas y Análisis de Sentimiento Detallado ---
    col_chart, col_sentiment = st.columns([2, 1])

    with col_chart:
        st.subheader("📈 Tendencias Recientes (Gráfico de Velas)")
        
        ohlc_data = st.session_state.get('yfinance_ohlc')
        
        if ohlc_data is not None and isinstance(ohlc_data.columns, pd.MultiIndex):
            
            cols_to_extract = [('Open', selected_ticker), ('High', selected_ticker), ('Low', selected_ticker), ('Close', selected_ticker)]
            
            # Verificamos que todas las columnas existan (Esto ahora está garantizado por get_valid_tickers_with_full_data)
            available_cols = [col for col in cols_to_extract if col in ohlc_data.columns]
            
            if len(available_cols) == 4:
                df_ohlc_ticker = ohlc_data.loc[:, available_cols].tail(30).copy()
                df_ohlc_ticker.columns = ['Open', 'High', 'Low', 'Close'] 
                
                if not df_ohlc_ticker.empty:
                    fig_candlestick = go.Figure(data=[go.Candlestick(
                        x=df_ohlc_ticker.index,
                        open=df_ohlc_ticker['Open'],
                        high=df_ohlc_ticker['High'],
                        low=df_ohlc_ticker['Low'],
                        close=df_ohlc_ticker['Close'],
                        increasing_line_color='green', 
                        decreasing_line_color='red'
                    )])

                    fig_candlestick.update_layout(
                        title=f'Gráfico de Velas (Últimos 30 días)',
                        xaxis_title='Fecha',
                        yaxis_title='Precio (USD)',
                        xaxis_rangeslider_visible=False,
                        height=400
                    )
                    st.plotly_chart(fig_candlestick, use_container_width=True)
                else:
                    st.warning(f"Datos OHLC recientes insuficientes para {selected_ticker}.")
            else:
                st.warning(f"No se pudieron cargar los datos de Open/High/Low/Close para {selected_ticker}. (Faltan columnas de yfinance)")

        else:
            st.warning(f"No se pudieron cargar los datos de Open/High/Low/Close para {selected_ticker}. (Problema de MultiIndex/Descarga)")

    with col_sentiment:
        st.subheader("📰 Análisis de Sentimiento (IA)")
        
        negative = sentiment_data['negative_pct']
        neutral = sentiment_data['neutral_pct']

        st.metric("Sentimiento Positivo", f"{positive:.1f}%")
        st.metric("Sentimiento Negativo", f"{negative:.1f}%")
        st.metric("Sentimiento Neutro", f"{neutral:.1f}%")
        
        st.markdown("---")
        st.caption("**Titulares Recientes Analizados:**")
        
        for news in sentiment_data['news_list']:
            color = "green" if news['label'] == 'Positivo' else "red" if news['label'] == 'Negativo' else "orange"
            st.markdown(f'<span style="color:{color};">●</span> {news["title"]}', unsafe_allow_html=True)


# -------------------------------------------------------------
# --- ESTRUCTURA PRINCIPAL DE LA APLICACIÓN (Navegación) ---
# -------------------------------------------------------------

st.set_page_config(layout="wide", page_title="Dashboard S&P 500 Interactivo")

if 'page' not in st.session_state:
    st.session_state.page = 'home'
    
if 'yfinance_ohlc' not in st.session_state:
     # Aseguramos que los datos de yfinance se carguen al inicio
     load_sp500_tickers() 

with st.sidebar:
    st.title("🧭 Navegación")
    
    if st.button("Página de Inicio", key='home_btn'):
        st.session_state.page = 'home'
        
    st.markdown("---")
    
    if st.button("Informe General", key='general_btn'):
        st.session_state.page = 'general'
        
    if st.button("Informe por Acción", key='stock_btn'):
        st.session_state.page = 'stock'

    st.markdown("---")
    if st.button("Análisis en Tiempo Real (IA)", key='realtime_btn'):
        st.session_state.page = 'realtime'

if st.session_state.page == 'home':
    page_home()
elif st.session_state.page == 'general':
    page_general_dashboard()
elif st.session_state.page == 'stock':
    page_stock_dashboard()
elif st.session_state.page == 'realtime':
    page_real_time_analysis()