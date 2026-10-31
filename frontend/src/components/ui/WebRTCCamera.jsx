import React, { useEffect, useRef, useState } from 'react';
import Card from './Card';

// Ponawianie polaczenia WHEP: 2 s, 4 s, 8 s, ... najwyzej co 30 s. Po udanym polaczeniu licznik wraca do zera.
// Dzieki temu kafelek sam ozywa, gdy robot zacznie nadawac, i sam wraca po zerwaniu polaczenia.
const RETRY_MIN_MS = 2000;
const RETRY_MAX_MS = 30000;

const STATUS_TEXT = {
    connecting: 'Connecting…',
    waiting: 'No stream, retrying…',
    playing: '',
};

const WebRTCCamera = ({ title, streamPath }) => {
    const videoRef = useRef(null);
    const [status, setStatus] = useState('connecting');

    useEffect(() => {
        // Ten sam origin co aplikacja: reverse proxy kieruje /video/* do MediaMTX (w dev: proxy Vite).
        const whepUrl = `/${streamPath}/whep`;
        let pc = null;
        let sessionUrl = null;
        let timer = null;
        let attempt = 0;
        let stopped = false;

        const closePeer = () => {
            if (pc) {
                pc.onconnectionstatechange = null;
                pc.ontrack = null;
                pc.close();
                pc = null;
            }
            if (sessionUrl) {
                // Zwolnij sesje po stronie MediaMTX; bez tego wygasa dopiero po timeoucie.
                fetch(sessionUrl, { method: 'DELETE', keepalive: true }).catch(() => {});
                sessionUrl = null;
            }
        };

        const scheduleRetry = () => {
            if (stopped) return;
            closePeer();
            const delay = Math.min(RETRY_MIN_MS * 2 ** attempt, RETRY_MAX_MS);
            attempt += 1;
            setStatus('waiting');
            timer = setTimeout(connect, delay);
        };

        const connect = async () => {
            if (stopped) return;
            setStatus('connecting');
            pc = new RTCPeerConnection();
            const current = pc;

            // Tylko odbior wideo, nic nie wysylamy.
            current.addTransceiver('video', { direction: 'recvonly' });

            current.ontrack = (event) => {
                if (videoRef.current && event.streams[0]) {
                    videoRef.current.srcObject = event.streams[0];
                }
            };

            current.onconnectionstatechange = () => {
                if (pc !== current) return;
                const state = current.connectionState;
                if (state === 'connected') {
                    attempt = 0;
                    setStatus('playing');
                } else if (state === 'failed' || state === 'disconnected' || state === 'closed') {
                    scheduleRetry();
                }
            };

            try {
                const offer = await current.createOffer();
                await current.setLocalDescription(offer);

                const response = await fetch(whepUrl, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/sdp' },
                    body: offer.sdp,
                });
                if (!response.ok) throw new Error(`MediaMTX returned ${response.status}`);
                if (stopped || pc !== current) return;

                const location = response.headers.get('Location');
                sessionUrl = location ? new URL(location, window.location.href).toString() : null;

                const answerSdp = await response.text();
                await current.setRemoteDescription({ type: 'answer', sdp: answerSdp });
            } catch (err) {
                if (!stopped && pc === current) {
                    console.warn(`Stream ${streamPath}: ${err.message}`);
                    scheduleRetry();
                }
            }
        };

        connect();

        return () => {
            stopped = true;
            clearTimeout(timer);
            closePeer();
            if (videoRef.current) videoRef.current.srcObject = null;
        };
    }, [streamPath]);

    return (
        <Card title={title}>
            <div style={{ position: 'relative', width: '100%', height: '100%', background: '#000', display: 'flex', alignItems: 'center', justifyContent: 'center', overflow: 'hidden' }}>
                <video
                    ref={videoRef}
                    autoPlay
                    playsInline
                    muted
                    style={{ width: '100%', height: '100%', objectFit: 'contain' }}
                />
                {status !== 'playing' && (
                    <div style={{ position: 'absolute', bottom: 8, left: 8, color: '#888', fontSize: 12, fontFamily: 'monospace' }}>
                        {STATUS_TEXT[status]}
                    </div>
                )}
            </div>
        </Card>
    );
};

export default WebRTCCamera;
