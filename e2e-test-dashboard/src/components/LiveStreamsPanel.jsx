import { useState, useEffect, useRef } from 'react';
import Hls from 'hls.js';
import { Maximize2, X, Clock } from 'lucide-react';
import { MOCK_CAMERAS, findHlsUrl } from '../data/cameras';

// Camera Card Component - Compact for right sidebar
function CameraCard({ camera, onMaximize }) {
  const videoRef = useRef(null);
  const hlsRef = useRef(null);
  const [status, setStatus] = useState('LOADING');
  const [showMaximize, setShowMaximize] = useState(false);
  const [currentTime, setCurrentTime] = useState(new Date());

  useEffect(() => {
    const timer = setInterval(() => setCurrentTime(new Date()), 1000);
    return () => clearInterval(timer);
  }, []);

  useEffect(() => {
    if (!videoRef.current) return;

    const initHls = async () => {
      try {
        const hlsUrl = findHlsUrl(camera.id);

        if (Hls.isSupported()) {
          const hls = new Hls({
            autoStartLoad: true,
            startPosition: -1,
            manifestLoadingTimeOut: 20000,
            manifestLoadingMaxRetry: 3,
            manifestLoadingRetryDelay: 1000,
          });

          hls.loadSource(hlsUrl);
          hls.attachMedia(videoRef.current);
          hlsRef.current = hls;

          hls.on(Hls.Events.MANIFEST_PARSED, () => {
            videoRef.current.play().catch(() => {
              setStatus('LOADING');
            });
            setStatus('NORMAL');
          });

          hls.on(Hls.Events.ERROR, (event, data) => {
            if (data.fatal) {
              setStatus('OFFLINE');
            }
          });

          return () => {
            if (hls) {
              hls.destroy();
            }
          };
        } else if (videoRef.current.canPlayType('application/vnd.apple.mpegurl')) {
          videoRef.current.src = hlsUrl;
          videoRef.current.addEventListener('loadedmetadata', () => setStatus('NORMAL'));
          videoRef.current.addEventListener('error', () => setStatus('OFFLINE'));
          videoRef.current.play().catch(() => setStatus('LOADING'));
        }
      } catch (error) {
        console.error(`Failed to initialize HLS for ${camera.id}:`, error);
        setStatus('OFFLINE');
      }
    };

    initHls();

    return () => {
      if (hlsRef.current) {
        hlsRef.current.destroy();
        hlsRef.current = null;
      }
    };
  }, [camera.id]);

  const statusColor = {
    NORMAL: 'bg-emerald-500/20 text-emerald-400',
    OFFLINE: 'bg-slate-500/20 text-slate-400',
    LOADING: 'bg-yellow-500/20 text-yellow-400',
  }[status];

  const timeStr = currentTime.toLocaleTimeString('en-US', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  });

  return (
    <div
      className="bg-slate-900/50 rounded-xl border border-slate-800 hover:border-emerald-500/50 shadow-lg overflow-hidden transition-all duration-200"
      onMouseEnter={() => setShowMaximize(true)}
      onMouseLeave={() => setShowMaximize(false)}
    >
      <div className="relative aspect-video bg-slate-950">
        <video
          ref={videoRef}
          className="w-full h-full object-cover bg-slate-950"
          muted
          autoPlay
          controls={false}
        />

        {/* Status Badge */}
        <div className="absolute top-2 right-2 flex items-center gap-1">
          <div className={`px-2 py-1 rounded-full text-xs font-medium ${statusColor}`}>
            {status}
          </div>
        </div>

        {/* Maximize Button */}
        {showMaximize && (
          <button
            onClick={() => onMaximize(camera)}
            className="absolute top-2 left-2 p-1 bg-black/50 hover:bg-black/75 rounded-lg transition-colors"
            title="Fullscreen"
          >
            <Maximize2 size={16} className="text-emerald-400" />
          </button>
        )}

        {/* Location Overlay */}
        <div className="absolute bottom-0 left-0 right-0 bg-gradient-to-t from-black/80 via-black/40 to-transparent p-3">
          <p className="text-sm font-medium text-slate-100">{camera.name}</p>
          <p className="text-xs text-slate-400">{camera.location}</p>
        </div>

        {/* Real-time Clock */}
        <div className="absolute bottom-2 right-2 flex items-center gap-1 bg-black/50 px-2 py-1 rounded-lg">
          <Clock size={12} className="text-slate-400" />
          <span className="text-xs font-mono text-slate-300">{timeStr}</span>
        </div>
      </div>
    </div>
  );
}

// Fullscreen Modal Component
function FocusModal({ camera, onClose }) {
  const videoRef = useRef(null);
  const hlsRef = useRef(null);
  const [status, setStatus] = useState('LOADING');
  const [currentTime, setCurrentTime] = useState(new Date());

  useEffect(() => {
    const timer = setInterval(() => setCurrentTime(new Date()), 1000);
    return () => clearInterval(timer);
  }, []);

  useEffect(() => {
    if (!videoRef.current) return;

    const initHls = async () => {
      try {
        const hlsUrl = findHlsUrl(camera.id);

        if (Hls.isSupported()) {
          const hls = new Hls({
            manifestLoadingTimeOut: 20000,
            manifestLoadingMaxRetry: 3,
          });
          hls.loadSource(hlsUrl);
          hls.attachMedia(videoRef.current);
          hlsRef.current = hls;

          hls.on(Hls.Events.MANIFEST_PARSED, () => {
            videoRef.current.play().catch(() => {
              setStatus('LOADING');
            });
            setStatus('NORMAL');
          });

          hls.on(Hls.Events.ERROR, (event, data) => {
            if (data.fatal) {
              setStatus('OFFLINE');
            }
          });
        } else if (videoRef.current.canPlayType('application/vnd.apple.mpegurl')) {
          videoRef.current.src = hlsUrl;
          videoRef.current.addEventListener('loadedmetadata', () => setStatus('NORMAL'));
          videoRef.current.addEventListener('error', () => setStatus('OFFLINE'));
          videoRef.current.play().catch(() => setStatus('LOADING'));
        }
      } catch (error) {
        console.error(`Failed to initialize HLS modal for ${camera.id}:`, error);
        setStatus('OFFLINE');
      }
    };

    initHls();

    return () => {
      if (hlsRef.current) {
        hlsRef.current.destroy();
        hlsRef.current = null;
      }
    };
  }, [camera.id]);

  const statusColor = {
    NORMAL: 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/50',
    OFFLINE: 'bg-slate-500/20 text-slate-400 border border-slate-500/50',
    LOADING: 'bg-yellow-500/20 text-yellow-400 border border-yellow-500/50',
  }[status];

  const timeStr = currentTime.toLocaleTimeString('en-US', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  });

  return (
    <div className="fixed inset-0 bg-black/80 backdrop-blur-sm flex items-center justify-center z-50">
      <div className="bg-slate-900 rounded-xl shadow-2xl w-[90vw] h-[90vh] max-w-4xl flex flex-col border border-slate-800">
        {/* Header */}
        <div className="flex items-center justify-between p-4 border-b border-slate-800">
          <div>
            <h2 className="text-xl font-semibold text-slate-100">{camera.name}</h2>
            <p className="text-sm text-slate-400">{camera.location}</p>
          </div>
          <button
            onClick={onClose}
            className="p-2 hover:bg-slate-800 rounded-lg transition-colors"
            title="Close (Esc)"
          >
            <X size={20} className="text-slate-400 hover:text-slate-200" />
          </button>
        </div>

        {/* Video Container */}
        <div className="flex-1 flex flex-col items-center justify-center bg-slate-950 relative overflow-hidden">
          <video
            ref={videoRef}
            className="w-full h-full object-contain"
            muted
            autoPlay
            controls
          />
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between p-4 border-t border-slate-800 bg-slate-950">
          <div className={`px-3 py-1 rounded-full text-sm font-medium ${statusColor}`}>
            {status}
          </div>
          <div className="flex items-center gap-2 text-slate-400">
            <Clock size={14} />
            <span className="text-sm font-mono">{timeStr}</span>
          </div>
        </div>
      </div>
    </div>
  );
}

// Main Live Streams Panel Component
export default function LiveStreamsPanel() {
  const [selectedCamera, setSelectedCamera] = useState(null);

  const handleMaximize = (camera) => {
    setSelectedCamera(camera);
  };

  const handleCloseModal = () => {
    setSelectedCamera(null);
  };

  // Close modal on Esc key
  useEffect(() => {
    const handleKeyDown = (e) => {
      if (e.key === 'Escape' && selectedCamera) {
        handleCloseModal();
      }
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [selectedCamera]);

  return (
    <div className="flex flex-col h-full gap-4 p-4 overflow-y-auto">
      <div className="flex items-center justify-between">
        <h3 className="text-lg font-semibold text-slate-100">Live Streams</h3>
        <span className="text-xs text-slate-500">{MOCK_CAMERAS.length} cameras</span>
      </div>

      {/* Camera Grid */}
      <div className="grid grid-cols-2 gap-3 auto-rows-max">
        {MOCK_CAMERAS.map((camera) => (
          <CameraCard
            key={camera.id}
            camera={camera}
            onMaximize={handleMaximize}
          />
        ))}
      </div>

      {/* Fullscreen Modal */}
      {selectedCamera && (
        <FocusModal
          camera={selectedCamera}
          onClose={handleCloseModal}
        />
      )}
    </div>
  );
}
