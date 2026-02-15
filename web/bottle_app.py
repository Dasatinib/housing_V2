# Optimized Housing Map V2
from bottle import route, run, default_app, request, response, static_file
import pandas as pd
import json
import os
import sshtunnel
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from datetime import datetime, date
import time
import threading
import atexit
from dotenv import load_dotenv

# Import DB config from local module
try:
    from sql_operations import DBconfig
except ImportError:
    # Fallback if sql_operations is not in path or has issues
    class DBconfig:
        def __init__(self):
            self.ssh_host = os.getenv("DB_SSH_HOST")
            self.ssh_username = os.getenv("DB_USR")
            self.ssh_pass = os.getenv("DB_PASS")
            self.ssh_pkey = os.getenv("DB_SSH_FILE")
            self.db_address = os.getenv("DB_HOST")
            self.db_name = os.getenv("DB_NAME_MASTER")
            self.db_is_local = os.getenv("DB_IS_LOCAL")

load_dotenv()

# Global state
_engine = None
_ssh_tunnel = None
_db_config = DBconfig()

def get_db_engine():
    """Get or create the SQLAlchemy engine with persistent SSH tunnel if needed"""
    global _engine, _ssh_tunnel, _db_config

    if _engine:
        return _engine

    print("Initializing database connection...")
    
    try:
        if _db_config.db_is_local == "true":
            print("Using direct connection (Local mode)")
            db_url = f"mysql+pymysql://{_db_config.ssh_username}:{_db_config.ssh_pass}@{_db_config.db_address}:3306/{_db_config.db_name}"
        else:
            print("Establishing SSH Tunnel...")
            # Ensure we don't start multiple tunnels
            if _ssh_tunnel is None:
                _ssh_tunnel = sshtunnel.SSHTunnelForwarder(
                    (_db_config.ssh_host),
                    ssh_username=_db_config.ssh_username,
                    ssh_pkey=_db_config.ssh_pkey,
                    remote_bind_address=(_db_config.db_address, 3306)
                )
                _ssh_tunnel.start()
                print(f"✅ SSH Tunnel started on port {_ssh_tunnel.local_bind_port}")

            db_url = f"mysql+pymysql://{_db_config.ssh_username}:{_db_config.ssh_pass}@127.0.0.1:{_ssh_tunnel.local_bind_port}/{_db_config.db_name}"

        # Create engine with connection pooling
        _engine = create_engine(
            db_url,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_recycle=3600,
            pool_pre_ping=True
        )
        print("✅ Database engine created")
        return _engine

    except Exception as e:
        print(f"❌ Database connection initialization failed: {e}")
        if _ssh_tunnel:
            _ssh_tunnel.stop()
            _ssh_tunnel = None
        return None

def cleanup():
    """Cleanup resources on shutdown"""
    global _engine, _ssh_tunnel
    if _engine:
        _engine.dispose()
    if _ssh_tunnel:
        print("Stopping SSH Tunnel...")
        _ssh_tunnel.stop()

atexit.register(cleanup)

def format_date(date_obj):
    """Format date object or string to readable format"""
    if not date_obj:
        return ""
    try:
        if isinstance(date_obj, str):
            # Try parsing yymmdd or yyyy-mm-dd
            if len(date_obj) == 6:
                return f"{date_obj[4:6]}.{date_obj[2:4]}.20{date_obj[0:2]}"
            if '-' in date_obj:
                d = datetime.strptime(date_obj, '%Y-%m-%d')
                return d.strftime('%d.%m.%Y')
        if isinstance(date_obj, (datetime, pd.Timestamp, date)):
            return date_obj.strftime('%d.%m.%Y')
        return str(date_obj)
    except:
        return str(date_obj)

def create_efficient_popup(group_data, images_list, is_available):
    """Create content for the property sidebar"""
    # group_data is a DataFrame containing all history for a specific location (lat/lng)
    
    # Sort by date descending (newest first)
    group_data = group_data.sort_values('Date obtained', ascending=False)
    
    # Latest entry
    latest = group_data.iloc[0]
    listing_id = latest['listing_id']
    
    # Calculate dates
    first_seen = group_data['Date obtained'].min()
    last_seen = group_data['Date obtained'].max()
    
    # Calculate total price
    rent = latest['Rent (CZK)'] or 0
    utilities = latest['Utilities (CZK)'] or 0
    services = latest['Services (CZK)'] or 0
    total_price = rent + utilities + services
    fee = latest['Fee'] if pd.notna(latest['Fee']) else 0
    
    formatted_last_seen = format_date(last_seen)
    formatted_first_seen = format_date(first_seen)
    
    # Availability Text
    availability_text = f"Available from <b>{formatted_first_seen}</b> to "
    if is_available:
        availability_text += "<b style='color:#28a745'>Still available</b>"
    else:
        availability_text += f"<b>{formatted_last_seen}</b>"

    # Image handling (Carousel/Scrollable)
    image_html = ""
    if images_list:
        image_items = ""
        for img_path in images_list:
            if img_path:
                image_items += f"<div style='flex:0 0 auto;width:200px;height:150px;margin-right:5px;background-image:url(https://images.najemchytre.cz/{img_path});background-size:cover;background-position:center;border-radius:4px;cursor:pointer;' onclick='window.open(\"https://images.najemchytre.cz/{img_path}\", \"_blank\")'></div>"
        
        if image_items:
            image_html = f"<div style='display:flex;overflow-x:auto;padding-bottom:5px;margin-bottom:12px;'>{image_items}</div>"
    
    url_link = ""
    if pd.notna(latest['URL']):
        url_link = f"<br><a href='{latest['URL']}' target='_blank' style='display:inline-block;margin-top:10px;padding:8px 16px;background:#007bff;color:white;text-decoration:none;border-radius:4px;font-weight:bold'>🔗 View on Source</a>"

    disposition = latest['Disposition'] if pd.notna(latest['Disposition']) else 'N/A'
    description = latest['Description'] if pd.notna(latest['Description']) else 'No description available.'
    address = latest['Address'] if pd.notna(latest['Address']) else 'Address not available'
    
    popup_content = f"""
    <div style='font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif; width:100%;'>
        <h2 style='margin:0 0 10px 0; font-size:24px; color:#333;'>{disposition} • {latest['Area (m2)']:.0f} m²</h2>
        <div style='font-size:16px;color:#555;margin-bottom:15px;display:flex;align-items:center'>
            📍 {address}
        </div>

        {image_html}
        
        <div style='background:#e9ecef;padding:15px;border-radius:8px;margin-bottom:15px;'>
            <div style='font-size:28px;color:#28a745;font-weight:bold;margin-bottom:5px'>
                {total_price:,.0f} CZK
            </div>
            <div style='font-size:14px;color:#666;'>
                Rent: <b>{rent:,.0f}</b> | Utils: <b>{utilities+services:,.0f}</b> | Fee: <b>{fee:,.0f}</b>
            </div>
        </div>
        
        <div style='margin-bottom:15px;padding:10px;background:#f8f9fa;border-left:4px solid #007bff;font-size:14px;'>
            {availability_text}
        </div>
        
        <div style='margin-bottom:15px;'>
            <h4 style='margin:0 0 5px 0;color:#333'>Description</h4>
            <div style='font-size:14px;line-height:1.5;color:#444;white-space:pre-wrap;'>{description}</div>
        </div>
    """
    
    # History section - Chart placeholder
    if len(group_data) > 1:
        popup_content += f"""
        <div style='margin-top:20px; padding-top:15px; border-top:1px solid #eee;'>
            <h4 style='margin:0 0 10px 0'>Price History</h4>
            <div style="height: 200px; width: 100%;">
                <canvas id="priceChart"></canvas>
            </div>
        </div>
        """

    popup_content += f"""
        <div style='font-size:12px;color:#888;border-top:1px solid #eee;padding-top:10px;margin-top:15px'>
            <div>ID: {listing_id}</div>
            {url_link}
        </div>
    </div>
    """
    return popup_content

@route('/api/properties')
def get_properties_api():
    """API endpoint to get properties within viewport bounds with filters"""
    response.content_type = 'application/json'

    try:
        # Viewport params
        lat_min = float(request.query.get('lat_min', 0))
        lat_max = float(request.query.get('lat_max', 0))
        lng_min = float(request.query.get('lng_min', 0))
        lng_max = float(request.query.get('lng_max', 0))
        limit = int(request.query.get('limit', 2000))

        # Filter params
        price_min = request.query.get('price_min')
        price_max = request.query.get('price_max')
        area_min = request.query.get('area_min')
        area_max = request.query.get('area_max')
        fee_max = request.query.get('fee_max')
        dispositions = request.query.get('dispositions') # Comma separated
        status_filter = request.query.get('status') # 'available', 'unavailable', or both/none

        engine = get_db_engine()
        if not engine:
            return json.dumps({"error": "Database connection failed"})

        # Build Query
        where_clauses = [
            "p.Latitude BETWEEN :lat_min AND :lat_max",
            "p.Longitude BETWEEN :lng_min AND :lng_max",
            "p.Latitude IS NOT NULL",
            "p.Longitude IS NOT NULL"
        ]
        
        params = {
            "lat_min": lat_min, "lat_max": lat_max, 
            "lng_min": lng_min, "lng_max": lng_max, 
            "limit": limit
        }

        # Apply Filters
        if price_min:
            where_clauses.append("(COALESCE(p.`Rent (CZK)`,0) + COALESCE(p.`Utilities (CZK)`,0) + COALESCE(p.`Services (CZK)`,0)) >= :price_min")
            params['price_min'] = int(price_min)
        if price_max:
            where_clauses.append("(COALESCE(p.`Rent (CZK)`,0) + COALESCE(p.`Utilities (CZK)`,0) + COALESCE(p.`Services (CZK)`,0)) <= :price_max")
            params['price_max'] = int(price_max)
            
        if area_min:
            where_clauses.append("p.`Area (m2)` >= :area_min")
            params['area_min'] = int(area_min)
        if area_max:
            where_clauses.append("p.`Area (m2)` <= :area_max")
            params['area_max'] = int(area_max)
            
        if fee_max:
            where_clauses.append("p.Fee <= :fee_max")
            params['fee_max'] = int(fee_max)
            
        if dispositions:
            dispo_list = dispositions.split(',')
            dispo_keys = [f"d{i}" for i in range(len(dispo_list))]
            for k, v in zip(dispo_keys, dispo_list):
                params[k] = v
            
            in_clause = ", ".join([f":{k}" for k in dispo_keys])
            where_clauses.append(f"p.Disposition IN ({in_clause})")

        where_sql = " AND ".join(where_clauses)

        query = text(f"""
        SELECT 
            p.listing_id, p.Latitude, p.Longitude, 
            p.`Rent (CZK)`, p.`Area (m2)`, p.`Date obtained`, 
            p.URL, p.Disposition, p.`Utilities (CZK)`, p.`Services (CZK)`,
            p.Description, p.Fee, p.Address
        FROM properties p
        WHERE {where_sql}
        ORDER BY p.`Date obtained` DESC
        LIMIT :limit
        """)

        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params=params)

        if df.empty:
            return json.dumps({"properties": [], "count": 0})

        # Fetch images for these properties
        listing_ids = df['listing_id'].unique().tolist()
        images_map = {}
        
        if listing_ids:
            listing_ids = [int(x) for x in listing_ids]
            ids_str = ','.join(map(str, listing_ids))
            img_query = text(f"SELECT listing_id, object_name FROM images WHERE listing_id IN ({ids_str})")
            
            with engine.connect() as conn:
                img_df = pd.read_sql(img_query, conn)
                
            for lid, group in img_df.groupby('listing_id'):
                images_map[lid] = group['object_name'].tolist()

        # Group by listing_id
        grouped = df.groupby('listing_id')
        properties = []
        today = datetime.now().date()
        
        # Status filters
        show_available = True
        show_unavailable = True
        if status_filter:
            statuses = status_filter.split(',')
            show_available = 'available' in statuses
            show_unavailable = 'unavailable' in statuses

        for listing_id, group in grouped:
            group = group.sort_values('Date obtained', ascending=False)
            latest = group.iloc[0]
            
            # Convert pandas timestamp to date
            latest_date = latest['Date obtained']
            if isinstance(latest_date, pd.Timestamp):
                latest_date = latest_date.date()
            elif isinstance(latest_date, str):
                 try:
                     latest_date = datetime.strptime(latest_date, '%Y-%m-%d').date()
                 except:
                     pass

            is_available = (latest_date == today)
            
            # Filter by status
            if is_available and not show_available:
                continue
            if not is_available and not show_unavailable:
                continue
            
            marker_type = 'single'
            imgs = images_map.get(listing_id, [])
            
            # Prepare history data for chart
            history = []
            if len(group) > 1:
                # We need ascending order for the chart
                chart_group = group.sort_values('Date obtained', ascending=True)
                for _, row in chart_group.iterrows():
                    d = row['Date obtained']
                    d_str = format_date(d)
                    price = (row['Rent (CZK)'] or 0) + (row['Utilities (CZK)'] or 0) + (row['Services (CZK)'] or 0)
                    history.append({'date': d_str, 'price': int(price)})

            properties.append({
                'lat': float(latest['Latitude']),
                'lng': float(latest['Longitude']),
                'type': marker_type,
                'is_available': is_available,
                'sidebar_html': create_efficient_popup(group, imgs, is_available),
                'history': history
            })

        return json.dumps({
            "properties": properties,
            "count": len(properties),
            "total_entries": len(df)
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return json.dumps({"error": str(e)})

@route('/')
def show_map():
    """Main map page"""
    engine = get_db_engine()
    if not engine:
        return "<h1>Error: Could not connect to database</h1>"

    try:
        # Get bounds and filters data
        with engine.connect() as conn:
            # Bounds and counts
            bounds_query = text("""
            SELECT
                MIN(Latitude) as lat_min, MAX(Latitude) as lat_max,
                MIN(Longitude) as lng_min, MAX(Longitude) as lng_max,
                AVG(Latitude) as lat_center, AVG(Longitude) as lng_center,
                COUNT(*) as total_data_points,
                COUNT(DISTINCT listing_id) as total_unique_properties
            FROM properties
            WHERE Latitude IS NOT NULL AND Longitude IS NOT NULL
            """)
            bounds_result = pd.read_sql(bounds_query, conn).iloc[0].to_dict()
            
            # Ensure counts are integers
            bounds = bounds_result
            bounds['total_data_points'] = int(bounds['total_data_points'])
            bounds['total_unique_properties'] = int(bounds['total_unique_properties'])
            
            # Dispositions for filter
            dispo_query = text("SELECT DISTINCT Disposition FROM properties WHERE Disposition IS NOT NULL AND Disposition != '' ORDER BY Disposition LIMIT 100")
            dispositions = pd.read_sql(dispo_query, conn)['Disposition'].tolist()
        
        # Build HTML
        dispo_options = "".join([f'<label><input type="checkbox" value="{d}"> {d}</label>' for d in dispositions])
        
        full_html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Housing Map V2</title>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=yes">
            
            <link rel="stylesheet" href="https://unpkg.com/leaflet@1.7.1/dist/leaflet.css" />
            <script src="https://unpkg.com/leaflet@1.7.1/dist/leaflet.js"></script>
            <link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.4.1/dist/MarkerCluster.css" />
            <link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.4.1/dist/MarkerCluster.Default.css" />
            <script src="https://unpkg.com/leaflet.markercluster@1.4.1/dist/leaflet.markercluster.js"></script>
            <!-- Chart.js -->
            <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>

            <style>
                body {{ margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif; overflow: hidden; }}
                
                #map-container {{
                    position: absolute; top: 0; left: 0;
                    width: calc(100% - 400px); height: 100vh;
                }}
                
                #map {{ width: 100%; height: 100%; }}
                
                /* Right Sidebar (Details) */
                #sidebar {{
                    position: absolute; top: 0; right: 0;
                    width: 400px; height: 100vh;
                    background: white; overflow-y: auto;
                    box-shadow: -2px 0 5px rgba(0,0,0,0.1);
                    z-index: 900;
                }}
                
                /* Left Sidebar (Filters) */
                #filter-sidebar {{
                    position: absolute; top: 170px; left: -320px;
                    width: 320px; height: calc(100vh - 180px);
                    background: white; overflow-y: auto;
                    box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
                    z-index: 1100;
                    transition: left 0.3s ease;
                    padding: 20px;
                    box-sizing: border-box;
                    border-radius: 0 8px 8px 0;
                    border: 1px solid #ddd;
                    border-left: none;
                }}
                
                #filter-sidebar.open {{ left: 0; }}
                
                .filter-header {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }}
                .filter-header h3 {{ margin: 0; }}
                .close-btn {{ background: none; border: none; font-size: 24px; cursor: pointer; color: #666; padding: 0 5px; }}
                .close-btn:hover {{ color: #000; }}

                .filter-group {{ margin-bottom: 20px; }}
                .filter-group label {{ display: block; font-weight: bold; margin-bottom: 5px; color: #333; }}
                .filter-row {{ display: flex; gap: 10px; align-items: center; }}
                .filter-row input {{ width: 100%; padding: 6px; border: 1px solid #ddd; border-radius: 4px; }}
                
                .checkbox-group {{ display: flex; flex-direction: column; gap: 5px; max-height: 200px; overflow-y: auto; border: 1px solid #eee; padding: 10px; border-radius: 4px; }}
                .checkbox-group label {{ font-weight: normal; font-size: 14px; cursor: pointer; }}
                
                /* Buttons */
                .btn {{ padding: 8px 12px; border: none; border-radius: 4px; cursor: pointer; font-weight: bold; }}
                .btn-primary {{ background: #007bff; color: white; }}
                .btn-primary:hover {{ background: #0056b3; }}
                .btn-secondary {{ background: #6c757d; color: white; }}
                
                .btn-toggle {{ 
                    position: absolute; top: 120px; left: 10px; z-index: 1000; 
                    background: white; border: 2px solid #ddd; padding: 8px 12px; 
                    border-radius: 4px; cursor: pointer; display: flex; align-items: center; 
                    gap: 5px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); 
                    transition: all 0.2s;
                }}
                
                .btn-toggle.active {{ 
                    border-color: #007bff; 
                    background-color: #e7f1ff;
                    color: #007bff;
                }}
                
                .btn-toggle .active-indicator {{
                    width: 8px; height: 8px; background-color: #dc3545; 
                    border-radius: 50%; display: none;
                }}
                .btn-toggle.active .active-indicator {{ display: block; }}
                
                .search-panel {{
                    position: absolute; top: 10px; left: 50px;
                    z-index: 1000;
                    background: white; padding: 10px; border-radius: 8px;
                    box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                    display: flex; flex-direction: column; gap: 5px;
                }}
                
                .search-box {{ display: flex; gap: 5px; }}
                input[type="text"] {{ padding: 8px; border: 1px solid #ddd; border-radius: 4px; width: 250px; }}
                
                #suggestions {{
                    background: white; border: 1px solid #ddd; border-radius: 4px;
                    max-height: 200px; overflow-y: auto; display: none;
                }}
                .suggestion-item {{ padding: 8px; cursor: pointer; border-bottom: 1px solid #eee; font-size: 14px; }}
                .suggestion-item:hover {{ background-color: #f8f9fa; }}
                
                .info-panel {{
                    position: absolute; top: 10px; left: 450px; z-index: 1000;
                    background: white; padding: 10px 15px; border-radius: 8px;
                    box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                    display: flex; flex-direction: column; gap: 5px;
                }}
                
                .info-stats {{
                    display: grid;
                    grid-template-columns: max-content auto;
                    column-gap: 10px;
                    align-items: center;
                    font-size: 12px;
                    color: #666;
                }}
                .info-stats div:nth-child(odd) {{ text-align: right; font-weight: bold; }}

                /* Markers */
                .property-marker {{ background: transparent; border: none; }}
                
                .marker-dot {{
                    width: 14px; height: 14px; border-radius: 50%;
                    background-color: #007bff; /* Fallback Blue */
                    border: 2px solid white;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.3); transition: all 0.2s ease;
                }}
                
                .marker-dot.available {{ background-color: #28a745; /* Green */ }}
                .marker-dot.unavailable {{ background-color: #dc3545; /* Red */ }}

                .marker-dot.selected {{ 
                    transform: scale(1.5); 
                    box-shadow: 0 0 0 4px rgba(0,0,0,0.2); 
                    z-index: 1000;
                }}
                
                .custom-cluster {{
                    background-color: rgba(220, 53, 69, 0.6); border-radius: 50%;
                    text-align: center; color: white; font-weight: bold;
                    border: 2px solid rgba(220, 53, 69, 1); line-height: 30px;
                }}
            </style>
        </head>
        <body>
            <!-- Filter Sidebar -->
            <div id="filter-sidebar">
                <div class="filter-header">
                    <h3>Filters</h3>
                    <button class="close-btn" onclick="toggleFilters()">&times;</button>
                </div>
                
                <div class="filter-group">
                    <label>Availability</label>
                    <div class="checkbox-group">
                        <label><input type="checkbox" id="chkAvailable" value="available" checked> Available (Green)</label>
                        <label><input type="checkbox" id="chkUnavailable" value="unavailable" checked> Not Available (Red)</label>
                    </div>
                </div>

                <div class="filter-group">
                    <label>Total Price (CZK)</label>
                    <div class="filter-row">
                        <input type="number" id="priceMin" placeholder="Min">
                        <span>-</span>
                        <input type="number" id="priceMax" placeholder="Max">
                    </div>
                </div>
                
                <div class="filter-group">
                    <label>Area (m²)</label>
                    <div class="filter-row">
                        <input type="number" id="areaMin" placeholder="Min">
                        <span>-</span>
                        <input type="number" id="areaMax" placeholder="Max">
                    </div>
                </div>
                
                <div class="filter-group">
                    <label>Max Fee (CZK)</label>
                    <input type="number" id="feeMax" placeholder="Max Fee">
                </div>
                
                <div class="filter-group">
                    <label>Disposition</label>
                    <div class="checkbox-group" id="dispoGroup">
                        {dispo_options}
                    </div>
                </div>
                
                <div style="display:flex; gap:10px;">
                    <button class="btn btn-primary" onclick="applyFilters()" style="flex:1">Apply</button>
                    <button class="btn btn-secondary" onclick="resetFilters()" style="flex:1">Reset</button>
                </div>
            </div>

            <!-- Main Content -->
            <button id="filterBtn" class="btn-toggle" onclick="toggleFilters()">
                <div class="active-indicator"></div>
                <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="4" y1="21" x2="4" y2="14"></line><line x1="4" y1="10" x2="4" y2="3"></line><line x1="12" y1="21" x2="12" y2="12"></line><line x1="12" y1="8" x2="12" y2="3"></line><line x1="20" y1="21" x2="20" y2="16"></line><line x1="20" y1="12" x2="20" y2="3"></line><line x1="1" y1="14" x2="7" y2="14"></line><line x1="9" y1="8" x2="15" y2="8"></line><line x1="17" y1="16" x2="23" y2="16"></line></svg>
                Filters
            </button>
            
            <div id="map-container">
                <div class="search-panel">
                    <div class="search-box">
                        <input type="text" id="addressInput" placeholder="Search address..." autocomplete="off">
                        <button class="btn btn-primary" onclick="performSearch()">Search</button>
                    </div>
                    <div id="suggestions"></div>
                </div>

                <div class="info-panel">
                    <h3 style="margin:0 0 5px 0">🏠 Housing V2</h3>
                    <div class="info-stats">
                        <div>{bounds['total_unique_properties']:,}</div> <div>properties loaded</div>
                        <div>{bounds['total_data_points']:,}</div> <div>data points</div>
                    </div>
                </div>
                
                <div id="map"></div>
            </div>
            
            <div id="sidebar">
                <div id="sidebar-content" style="padding:20px;">
                    <div style="display:flex;flex-direction:column;align-items:center;justify-content:center;height:80vh;color:#888;text-align:center;">
                        <svg width="64" height="64" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1" stroke-linecap="round" stroke-linejoin="round" style="margin-bottom:20px;opacity:0.5"><path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"></path><polyline points="9 22 9 12 15 12 15 22"></polyline></svg>
                        <h3 style="font-weight:normal">Select a listing<br>to view details</h3>
                    </div>
                </div>
            </div>

            <script>
            const map = L.map('map', {{
                preferCanvas: true
            }}).setView([{bounds['lat_center'] or 50.0755}, {bounds['lng_center'] or 14.4378}], 13);

            L.tileLayer('https://{{s}}.basemaps.cartocdn.com/rastertiles/voyager/{{z}}/{{x}}/{{y}}{{r}}.png', {{
                attribution: '© OpenStreetMap, © CARTO',
                maxZoom: 19
            }}).addTo(map);

            const markers = L.markerClusterGroup({{
                chunkedLoading: true, maxClusterRadius: 40, spiderfyOnMaxZoom: true,
                iconCreateFunction: function(cluster) {{
                    return L.divIcon({{
                        html: '<span>' + cluster.getChildCount() + '</span>',
                        className: 'custom-cluster',
                        iconSize: L.point(30, 30)
                    }});
                }}
            }});
            map.addLayer(markers);

            let loading = false;
            let selectedMarker = null;
            let priceChart = null;

            // --- FILTER LOGIC ---
            function toggleFilters() {{
                document.getElementById('filter-sidebar').classList.toggle('open');
            }}
            
            function getFilters() {{
                const dispos = Array.from(document.querySelectorAll('#dispoGroup input:checked')).map(cb => cb.value);
                
                const showAvail = document.getElementById('chkAvailable').checked;
                const showUnavail = document.getElementById('chkUnavailable').checked;
                const statusList = [];
                if (showAvail) statusList.push('available');
                if (showUnavail) statusList.push('unavailable');

                const filters = {{
                    price_min: document.getElementById('priceMin').value,
                    price_max: document.getElementById('priceMax').value,
                    area_min: document.getElementById('areaMin').value,
                    area_max: document.getElementById('areaMax').value,
                    fee_max: document.getElementById('feeMax').value,
                    dispositions: dispos.join(','),
                    status: statusList.join(',')
                }};
                
                // Update button indicator
                const hasFilters = filters.price_min || filters.price_max || filters.area_min || filters.area_max || filters.fee_max || filters.dispositions || (statusList.length < 2);
                const filterBtn = document.getElementById('filterBtn');
                if (hasFilters) {{
                    filterBtn.classList.add('active');
                }} else {{
                    filterBtn.classList.remove('active');
                }}
                
                return filters;
            }}
            
            function applyFilters() {{
                toggleFilters();
                loadProperties();
            }}
            
            function resetFilters() {{
                document.getElementById('priceMin').value = '';
                document.getElementById('priceMax').value = '';
                document.getElementById('areaMin').value = '';
                document.getElementById('areaMax').value = '';
                document.getElementById('feeMax').value = '';
                document.querySelectorAll('#dispoGroup input').forEach(cb => cb.checked = false);
                document.getElementById('chkAvailable').checked = true;
                document.getElementById('chkUnavailable').checked = true;
                
                getFilters(); // Update UI state
                loadProperties();
            }}

            // --- MAP LOADING ---
            function createIcon(isAvailable, isSelected) {{
                const colorClass = isAvailable ? 'available' : 'unavailable';
                const selectedClass = isSelected ? 'selected' : '';
                return L.divIcon({{
                    className: 'property-marker',
                    html: `<div class="marker-dot ${{colorClass}} ${{selectedClass}}"></div>`,
                    iconSize: [18, 18],
                    iconAnchor: [9, 9]
                }});
            }}

            function renderChart(history) {{
                const ctx = document.getElementById('priceChart');
                if (!ctx) return;
                
                if (priceChart) {{
                    priceChart.destroy();
                }}
                
                const dates = history.map(h => h.date);
                const prices = history.map(h => h.price);
                
                priceChart = new Chart(ctx, {{
                    type: 'line',
                    data: {{
                        labels: dates,
                        datasets: [{{
                            label: 'Total Price (CZK)',
                            data: prices,
                            borderColor: '#007bff',
                            backgroundColor: 'rgba(0, 123, 255, 0.1)',
                            borderWidth: 2,
                            tension: 0.1,
                            fill: true
                        }}]
                    }},
                    options: {{
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: {{
                            legend: {{ display: false }},
                            tooltip: {{
                                intersect: false,
                                mode: 'index',
                            }}
                        }},
                        scales: {{
                            y: {{
                                beginAtZero: false,
                                ticks: {{
                                    callback: function(value) {{ return (value/1000) + 'k'; }}
                                }}
                            }},
                            x: {{
                                display: false // Hide dates on x-axis if too many
                            }}
                        }}
                    }}
                }});
            }}

            function updateSidebar(html, historyData) {{
                const sidebar = document.getElementById('sidebar-content');
                sidebar.innerHTML = html;
                document.getElementById('sidebar').scrollTop = 0;
                
                // Render chart if history exists
                if (historyData && historyData.length > 0) {{
                    // Wait for DOM update
                    setTimeout(() => {{
                        renderChart(historyData);
                    }}, 50);
                }}
            }}
            
            function loadProperties() {{
                if (loading) return;
                loading = true;

                const bounds = map.getBounds();
                const filters = getFilters();
                
                let url = `/api/properties?lat_min=${{bounds.getSouth()}}&lat_max=${{bounds.getNorth()}}&lng_min=${{bounds.getWest()}}&lng_max=${{bounds.getEast()}}`;
                
                if(filters.price_min) url += `&price_min=${{filters.price_min}}`;
                if(filters.price_max) url += `&price_max=${{filters.price_max}}`;
                if(filters.area_min) url += `&area_min=${{filters.area_min}}`;
                if(filters.area_max) url += `&area_max=${{filters.area_max}}`;
                if(filters.fee_max) url += `&fee_max=${{filters.fee_max}}`;
                if(filters.dispositions) url += `&dispositions=${{encodeURIComponent(filters.dispositions)}}`;
                if(filters.status) url += `&status=${{filters.status}}`;

                fetch(url)
                    .then(r => r.json())
                    .then(data => {{
                        markers.clearLayers();
                        if (data.properties) {{
                            const newLayers = data.properties.map(p => {{
                                const marker = L.marker([p.lat, p.lng], {{icon: createIcon(p.is_available, false)}});
                                marker.options.sidebarHtml = p.sidebar_html;
                                marker.options.isAvailable = p.is_available;
                                marker.options.history = p.history;
                                
                                marker.on('click', function(e) {{
                                    if (selectedMarker) {{
                                        selectedMarker.setIcon(createIcon(selectedMarker.options.isAvailable, false));
                                    }}
                                    
                                    this.setIcon(createIcon(this.options.isAvailable, true));
                                    selectedMarker = this;
                                    
                                    updateSidebar(this.options.sidebarHtml, this.options.history);
                                }});
                                return marker;
                            }});
                            markers.addLayers(newLayers);
                        }}
                    }})
                    .catch(e => console.error(e))
                    .finally(() => loading = false);
            }}

            // --- SEARCH LOGIC ---
            const input = document.getElementById('addressInput');
            const suggestionsDiv = document.getElementById('suggestions');
            let debounceTimeout = null;

            input.addEventListener('input', function() {{
                const query = this.value;
                if (!query || query.length < 3) {{
                    suggestionsDiv.style.display = 'none';
                    return;
                }}
                clearTimeout(debounceTimeout);
                debounceTimeout = setTimeout(() => fetchSuggestions(query), 300);
            }});

            input.addEventListener('keydown', function(e) {{
                if (e.key === 'Enter') performSearch();
            }});

            document.addEventListener('click', function(e) {{
                if (e.target !== input && e.target !== suggestionsDiv) suggestionsDiv.style.display = 'none';
            }});

            function fetchSuggestions(query) {{
                fetch(`https://nominatim.openstreetmap.org/search?format=json&q=${{encodeURIComponent(query)}}&addressdetails=1&limit=5&countrycodes=cz`)
                    .then(r => r.json())
                    .then(data => showSuggestions(data))
                    .catch(e => console.error(e));
            }}

            function showSuggestions(data) {{
                suggestionsDiv.innerHTML = '';
                if (!data || data.length === 0) {{
                    suggestionsDiv.style.display = 'none';
                    return;
                }}
                data.forEach(item => {{
                    const div = document.createElement('div');
                    div.className = 'suggestion-item';
                    div.textContent = item.display_name;
                    div.onclick = () => {{
                        input.value = item.display_name;
                        suggestionsDiv.style.display = 'none';
                        map.setView([item.lat, item.lon], 15);
                    }};
                    suggestionsDiv.appendChild(div);
                }});
                suggestionsDiv.style.display = 'block';
            }}

            function performSearch() {{
                const query = input.value;
                if (!query) return;
                fetchSuggestions(query);
            }}

            let timeout;
            map.on('moveend', () => {{
                clearTimeout(timeout);
                timeout = setTimeout(loadProperties, 500);
            }});

            loadProperties();
            </script>
        </body>
        </html>
        """
        return full_html
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return f"<h1>Error loading map: {e}</h1>"

@route('/stats')
def show_stats():
    """Statistics page"""
    engine = get_db_engine()
    if not engine:
        return "<h1>Error: Could not connect to database</h1>"

    try:
        # Get comprehensive statistics
        stats_query = text("""
        SELECT
            COUNT(*) as total_records,
            COUNT(DISTINCT listing_id) as unique_properties,
            MIN(`Rent (CZK)`) as min_price,
            MAX(`Rent (CZK)`) as max_price,
            AVG(`Rent (CZK)`) as avg_price,
            MIN(`Area (m2)`) as min_surface,
            MAX(`Area (m2)`) as max_surface,
            AVG(`Area (m2)`) as avg_surface,
            MIN(Latitude) as lat_min,
            MAX(Latitude) as lat_max,
            MIN(Longitude) as lng_min,
            MAX(Longitude) as lng_max
        FROM properties
        WHERE Latitude IS NOT NULL AND Longitude IS NOT NULL
        """)

        with engine.connect() as conn:
            result = pd.read_sql(stats_query, conn).iloc[0].to_dict()
            stats = result

        return f"""
        <html>
        <head><title>Property Statistics</title></head>
        <body style='font-family: Arial, sans-serif; margin: 40px;'>
            <h1>📊 Property Database Statistics</h1>

            <div style='display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin: 20px 0;'>
                <div style='background: #f8f9fa; padding: 15px; border-radius: 8px;'>
                    <h2>📈 Data Overview</h2>
                    <ul>
                        <li><strong>Total Records:</strong> {stats['total_records']:,}</li>
                        <li><strong>Unique Properties:</strong> {stats['unique_properties']:,}</li>
                    </ul>
                </div>

                <div style='background: #f8f9fa; padding: 15px; border-radius: 8px;'>
                    <h2>💰 Price Range (Rent)</h2>
                    <ul>
                        <li><strong>Minimum:</strong> {stats['min_price']:,.0f}</li>
                        <li><strong>Maximum:</strong> {stats['max_price']:,.0f}</li>
                        <li><strong>Average:</strong> {stats['avg_price']:,.0f}</li>
                    </ul>
                </div>

                <div style='background: #f8f9fa; padding: 15px; border-radius: 8px;'>
                    <h2>📐 Surface Area</h2>
                    <ul>
                        <li><strong>Minimum:</strong> {stats['min_surface']:,.0f} m²</li>
                        <li><strong>Maximum:</strong> {stats['max_surface']:,.0f} m²</li>
                        <li><strong>Average:</strong> {stats['avg_surface']:,.0f} m²</li>
                    </ul>
                </div>

                <div style='background: #f8f9fa; padding: 15px; border-radius: 8px;'>
                    <h2>🗺️ Geographic Coverage</h2>
                    <ul>
                        <li><strong>Latitude:</strong> {stats['lat_min']:.4f} to {stats['lat_max']:.4f}</li>
                        <li><strong>Longitude:</strong> {stats['lng_min']:.4f} to {stats['lng_max']:.4f}</li>
                    </ul>
                </div>
            </div>

            <div style='text-align: center; margin: 30px 0;'>
                <a href='/' style='background: #007bff; color: white; padding: 12px 24px; text-decoration: none; border-radius: 5px; font-size: 16px;'>
                    🗺️ Back to Map
                </a>
            </div>
        </body>
        </html>
        """

    except Exception as e:
        import traceback
        traceback.print_exc()
        return f"<h1>Error: {str(e)}</h1>"

if __name__ == "__main__":
    run(host='localhost', port=8080, debug=True, reloader=True)
