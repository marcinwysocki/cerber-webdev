import React, { useEffect, useRef } from 'react';
import Card from './Card';

const WebRTCCamera = ({ title, streamPath }) => {
    const videoRef = useRef(null);
    const peerConnection = useRef(null);

    useEffect(() => {
        // Ten sam origin co aplikacja: reverse proxy kieruje /video/* do MediaMTX (w dev: proxy Vite).
        const whepUrl = `/${streamPath}/whep`;

        const startStream = async () => {
            peerConnection.current = new RTCPeerConnection();

            // We only want to receive video, no need to send anything
            peerConnection.current.addTransceiver('video', { direction: 'recvonly' });

            // Bind the incoming stream to our video element
            peerConnection.current.ontrack = (event) => {
                if (videoRef.current && event.streams[0]) {
                    videoRef.current.srcObject = event.streams[0];
                }
            };

            try {
                // 1. Create our local offer
                const offer = await peerConnection.current.createOffer();
                await peerConnection.current.setLocalDescription(offer);

                // 2. Send the offer to MediaMTX via WHEP
                const response = await fetch(whepUrl, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/sdp' },
                    body: offer.sdp
                });

                if (!response.ok) throw new Error(`MediaMTX returned ${response.status}`);

                // 3. Apply the remote answer
                const answerSdp = await response.text();
                await peerConnection.current.setRemoteDescription({
                    type: 'answer',
                    sdp: answerSdp
                });
            } catch (err) {
                console.error(`Failed to connect to stream ${streamPath}:`, err);
            }
        };

        startStream();

        return () => {
            if (peerConnection.current) {
                peerConnection.current.close();
            }
        };
    }, [streamPath]);

    return (
        <Card title={title}>
            <div style={{ width: '100%', height: '100%', background: '#000', display: 'flex', alignItems: 'center', justifyContent: 'center', overflow: 'hidden' }}>
                <video
                    ref={videoRef}
                    autoPlay
                    playsInline
                    muted
                    style={{ width: '100%', height: '100%', objectFit: 'contain' }}
                />
            </div>
        </Card>
    );
};

export default WebRTCCamera;
