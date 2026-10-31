import React, { useState, useEffect} from "react";

const GpsWidget = () => {
    const [gpsData, setGpsData] = useState({
        latitude: 0.0, 
        longitude: 0.0,
        altitude: 0.0, 
        date: "N/A", 
        satellites: 0,
        fix_quality: 0,
        is_valid: false
    });
    const [apiStatus, setApiStatus] = useState('Connecting...');

    useEffect(() => {
        const fetchGpsData = async () => {
            try {
                // Ten sam origin co aplikacja: reverse proxy kieruje /api/* do backendu (w dev: proxy Vite).
                const response = await fetch('/api/v1/mqtt/gps');
                if (!response.ok) {
                    throw new Error(`HTTP error! Status: ${response.status}`);
                }

                const data = await response.json();

                if (data && data.is_valid) {
                    setGpsData({ 
                        latitude: data.latitude, 
                        longitude: data.longitude,
                        altitude: data.altitude,     
                        date: data.date, 
                        satellites: data.satellites, 
                        fix_quality: data.fix_quality, 
                        is_valid: data.is_valid 
                    });
                    setApiStatus('Connected');
                } else {
                    setApiStatus('Invalid GPS fix');
                }
            } catch (error) {
                console.error("Error getting GPS data from the API:", error);
                setApiStatus('Connection Error');
            }
        };
        
        fetchGpsData();

        const intervalId = setInterval(fetchGpsData, 2000);

        return () => clearInterval(intervalId);
    }, []);

    return (
        <div className="widget-container" style={{ alignItems: 'center', justifyContent: 'center', display: 'flex', flexDirection: 'column' }}>            
            <div style={{ fontSize: '18px', fontFamily: 'monospace', margin: '4px 0' }}>
                <span style={{ color: 'var(--text-secondary)' }}>latitude: </span>
                <span style={{ fontWeight: 'bold', color: 'var(--text-primary)' }}>
                    {gpsData.latitude.toFixed(6)}
                </span>
            </div>
            
            <div style={{ fontSize: '18px', fontFamily: 'monospace', margin: '4px 0' }}>
                <span style={{ color: 'var(--text-secondary)' }}>longitude: </span>
                <span style={{ fontWeight: 'bold', color: 'var(--text-primary)' }}>
                    {gpsData.longitude.toFixed(6)}
                </span>
            </div>

            <div style={{ fontSize: '18px', fontFamily: 'monospace', margin: '4px 0' }}>
                <span style={{ color: 'var(--text-secondary)' }}>altitude: </span>
                <span style={{ fontWeight: 'bold', color: 'var(--text-primary)' }}>
                    {gpsData.altitude.toFixed(6)}
                </span>
            </div>

            <div style={{ fontSize: '18px', fontFamily: 'monospace', margin: '4px 0' }}>
                <span style={{ color: 'var(--text-secondary)' }}>satellites: </span>
                <span style={{ fontWeight: 'bold', color: 'var(--text-primary)' }}>
                    {gpsData.satellites}
                </span>
            </div>

            <div style={{ fontSize: '18px', fontFamily: 'monospace', margin: '4px 0' }}>
                <span style={{ color: 'var(--text-secondary)' }}>timestamp: </span>
                <span style={{ fontWeight: 'bold', color: 'var(--text-primary)' }}>
                    {gpsData.date}
                </span>
            </div>

            <div style={{ 
                marginTop: '16px', 
                fontSize: '12px', 
                color: apiStatus === 'Connected' ? 'var(--accent-blue)' : 'var(--accent-red)' 
            }}>
                API: {apiStatus}
            </div>
        </div>
    );
}

export default GpsWidget;