// import React, { useState, useEffect } from 'react';
// import { MOCK_CAMERAS } from '../../constants';
// import { Camera, CameraStatus } from '../../types';
// import { X, Maximize } from 'lucide-react';

// const getStatusBadgeColor = (status: CameraStatus) => {
//   switch (status) {
//     case CameraStatus.NORMAL:
//       return 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30';
//     case CameraStatus.VIOLENCE_DETECTED:
//       return 'bg-red-500/20 text-red-400 border-red-500/30 animate-pulse';
//     case CameraStatus.OFFLINE:
//       return 'bg-slate-500/20 text-slate-400 border-slate-500/30';
//     default:
//       return '';
//   }
// };

// const CameraCard: React.FC<{ camera: Camera; onFocus: (camera: Camera) => void }> = ({ camera, onFocus }) => {
//     const [time, setTime] = useState(new Date());

//     useEffect(() => {
//         const timer = setInterval(() => setTime(new Date()), 1000);
//         return () => clearInterval(timer);
//     }, []);

//     return (
//         <div className="bg-slate-900/50 rounded-xl overflow-hidden shadow-lg border border-slate-800 hover:border-emerald-500/50 transition-all duration-300 group">
//             <div className="relative">
//                 <img 
//                     src={`https://picsum.photos/seed/${camera.id}/400/225`} 
//                     alt={camera.specificLocation}
//                     className="w-full h-auto object-cover"
//                 />
//                 <div className="absolute top-2 right-2 opacity-0 group-hover:opacity-100 transition-opacity">
//                     <button onClick={() => onFocus(camera)} className="p-2 bg-slate-900/50 rounded-full text-white hover:bg-emerald-500">
//                         <Maximize size={16} />
//                     </button>
//                 </div>
//                 <div className="absolute bottom-0 left-0 w-full p-2 bg-gradient-to-t from-black/60 to-transparent">
//                      <p className="text-white font-semibold text-sm drop-shadow-md">{camera.specificLocation}</p>
//                      <p className="text-slate-300 text-xs drop-shadow-md">{`${camera.ward}, ${camera.district}`}</p>
//                 </div>
//             </div>
//             <div className="p-3 flex justify-between items-center">
//                 <span className={`px-2 py-1 text-xs font-medium rounded-full border ${getStatusBadgeColor(camera.status)}`}>
//                     {camera.status}
//                 </span>
//                 <span className="text-xs text-slate-400 font-mono">{time.toLocaleTimeString()}</span>
//             </div>
//         </div>
//     )
// }

// const FocusModal: React.FC<{ camera: Camera; onClose: () => void }> = ({ camera, onClose }) => (
//     <div className="fixed inset-0 bg-black/80 backdrop-blur-sm flex items-center justify-center z-50">
//         <div className="relative bg-slate-900 rounded-xl shadow-2xl w-full max-w-4xl border border-slate-700">
//              <div className="p-4 flex justify-between items-center border-b border-slate-800">
//                 <div>
//                     <h3 className="text-xl font-bold text-white">{camera.specificLocation}</h3>
//                     <p className="text-sm text-slate-400">{`${camera.ward}, ${camera.district}, ${camera.city}`}</p>
//                 </div>
//                 <button onClick={onClose} className="p-2 rounded-full hover:bg-slate-800">
//                     <X size={20} />
//                 </button>
//             </div>
//             <img 
//                 src={`https://picsum.photos/seed/${camera.id}/1280/720`} 
//                 alt={camera.specificLocation}
//                 className="w-full h-auto"
//             />
//             <div className="p-4 bg-slate-900/50 rounded-b-xl flex justify-between items-center">
//                  <span className={`px-3 py-1 text-sm font-medium rounded-full border ${getStatusBadgeColor(camera.status)}`}>
//                     {camera.status}
//                 </span>
//                 <span className="text-sm text-slate-400 font-mono">{new Date().toLocaleString()}</span>
//             </div>
//         </div>
//     </div>
// );

// const LiveStreams: React.FC = () => {
//   const [cameras] = useState<Camera[]>(MOCK_CAMERAS);
//   const [focusedCamera, setFocusedCamera] = useState<Camera | null>(null);

//   return (
//     <div>
//         <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6">
//             {cameras.map((camera) => (
//                 <CameraCard key={camera.id} camera={camera} onFocus={setFocusedCamera} />
//             ))}
//         </div>
//         {focusedCamera && <FocusModal camera={focusedCamera} onClose={() => setFocusedCamera(null)} />}
//     </div>
//   );
// };
import React, { useState, useEffect, useRef } from 'react';
import { MOCK_CAMERAS } from '../../constants';
import { Camera, CameraStatus } from '../../types';
import { X, Maximize } from 'lucide-react';
import Hls from 'hls.js'; // Đã thêm: Import Hls.js

// Địa chỉ Host của MediaMTX
const MEDIA_MTX_HOST = 'http://localhost:8888';

// --- Hàm hỗ trợ ---
const getStatusBadgeColor = (status: CameraStatus) => {
    switch (status) {
        case CameraStatus.NORMAL:
            return 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30';
        case CameraStatus.VIOLENCE_DETECTED:
            return 'bg-red-500/20 text-red-400 border-red-500/30 animate-pulse';
        case CameraStatus.OFFLINE:
            return 'bg-slate-500/20 text-slate-400 border-slate-500/30';
        default:
            return '';
    }
};

// Thử nhiều vị trí playlist HLS để tìm URL hợp lệ
const findWorkingHlsUrl = async (baseHost: string, camId: string): Promise<string | null> => {
  const candidates = [
    `${baseHost}/${camId}/index.m3u8`,
    `${baseHost}/${camId}.m3u8`,
    `${baseHost}/${camId}/stream.m3u8`,
    `${baseHost}/${camId}/`,
  ];

  for (const url of candidates) {
    try {
      // Dùng GET thật (không HEAD) và không cache để có kết quả chính xác
      const res = await fetch(url, { method: 'GET', cache: 'no-store' });
      if (!res.ok) continue;

      // Kiểm tra content-type xem có phải m3u8 không
      const ct = res.headers.get('content-type') || '';
      const text = await res.text();

      // Kiểm tra nội dung master playlist / m3u8
      if (ct.includes('mpegurl') || text.includes('#EXTM3U')) {
        return url;
      }
      // nếu url là directory (v.d. '/cam_01/') server trả HTML player — vẫn OK vì player sẽ load index.m3u8
      if (ct.includes('html')) {
        return url;
      }
    } catch (e) {
      // ignore và thử candidate tiếp theo
      console.debug('candidate GET failed', url, e);
    }
  }
  return null;
};


// --- Component CameraCard (Hiển thị luồng HLS nhỏ) ---
const CameraCard: React.FC<{ camera: Camera; onFocus: (camera: Camera) => void }> = ({ camera, onFocus }) => {
    const [time, setTime] = useState(new Date());
    const videoRef = useRef<HTMLVideoElement>(null); // Tham chiếu video
    const hlsRef = useRef<Hls | null>(null); // Tham chiếu HLS instance
    const resolvedUrlRef = useRef<string | null>(null);

    useEffect(() => {
        const videoElement = videoRef.current;
        if (!videoElement) return;

        let mounted = true;
        // ensure muted for autoplay in preview
        videoElement.muted = true;

        // cleanup any old hls instance synchronously
        if (hlsRef.current) {
            try { hlsRef.current.destroy(); } catch (_) {}
            hlsRef.current = null;
        }

        // Async: find working playlist URL
        (async () => {
            const url = await findWorkingHlsUrl(MEDIA_MTX_HOST, camera.id);
            if (!mounted) return;
            if (!url) {
                // không tìm thấy playlist hợp lệ
                console.warn(`No HLS playlist found for ${camera.id}`);
                return;
            }

            resolvedUrlRef.current = url;

            if (Hls.isSupported()) {
                const hls = new Hls();
                hlsRef.current = hls;

                const onMediaAttached = () => {
                    if (!mounted) return;
                    try {
                        hls.loadSource(url);
                    } catch (e) {
                        // ignore
                    }
                };

                const onManifestParsed = () => {
                    if (!mounted) return;
                    // cố gắng play, nhưng bỏ qua AbortError
                    videoElement.play().catch((err: any) => {
                        if (err && err.name !== 'AbortError') {
                            console.error(`Video play failed for ${camera.id}:`, err);
                        }
                    });
                };

                hls.on(Hls.Events.MEDIA_ATTACHED, onMediaAttached);
                hls.on(Hls.Events.MANIFEST_PARSED, onManifestParsed);

                hls.attachMedia(videoElement);

            } else if (videoElement.canPlayType('application/vnd.apple.mpegurl')) {
                videoElement.src = url;
                videoElement.play().catch((err: any) => {
                    if (err && err.name !== 'AbortError') console.error(`Native video play failed for ${camera.id}:`, err);
                });
            }
        })();

        // Cleanup function (quan trọng để tránh rò rỉ bộ nhớ)
        return () => {
            mounted = false;
            if (hlsRef.current) {
                try { hlsRef.current.stopLoad && (hlsRef.current as any).stopLoad(); } catch (_) {}
                try { hlsRef.current.destroy(); } catch (_) {}
                hlsRef.current = null;
            }
            try { videoElement.pause(); } catch (_) {}
        };
    }, [camera.id]); // Chạy lại khi camera.id thay đổi

    useEffect(() => {
        const timer = setInterval(() => setTime(new Date()), 1000);
        return () => clearInterval(timer);
    }, []);

    return (
        <div className="bg-slate-900/50 rounded-xl overflow-hidden shadow-lg border border-slate-800 hover:border-emerald-500/50 transition-all duration-300 group">
            <div className="relative aspect-video"> {/* Đảm bảo tỷ lệ khung hình */}
                <video
                    ref={videoRef}
                    autoPlay
                    muted // Phải có muted để đảm bảo tự động phát
                    loop
                    className="w-full h-full object-cover"
                    // Dùng ảnh tĩnh làm placeholder nếu video chưa tải kịp
                    poster={`https://picsum.photos/seed/${camera.id}/400/225`} 
                />
                <div className="absolute top-2 right-2 opacity-0 group-hover:opacity-100 transition-opacity">
                    <button onClick={() => onFocus(camera)} className="p-2 bg-slate-900/50 rounded-full text-white hover:bg-emerald-500">
                        <Maximize size={16} />
                    </button>
                </div>
                <div className="absolute bottom-0 left-0 w-full p-2 bg-gradient-to-t from-black/60 to-transparent">
                     <p className="text-white font-semibold text-sm drop-shadow-md">{camera.specificLocation}</p>
                     <p className="text-slate-300 text-xs drop-shadow-md">{`${camera.ward}, ${camera.district}`}</p>
                </div>
            </div>
            <div className="p-3 flex justify-between items-center">
                <span className={`px-2 py-1 text-xs font-medium rounded-full border ${getStatusBadgeColor(camera.status)}`}>
                    {camera.status}
                </span>
                <span className="text-xs text-slate-400 font-mono">{time.toLocaleTimeString()}</span>
            </div>
        </div>
    )
}

// --- Component FocusModal (Hiển thị luồng HLS lớn) ---
const FocusModal: React.FC<{ camera: Camera; onClose: () => void }> = ({ camera, onClose }) => {
    const videoRef = useRef<HTMLVideoElement>(null);
    const hlsRef = useRef<Hls | null>(null);

    useEffect(() => {
        const videoElement = videoRef.current;
        if (!videoElement) return;

        let mounted = true;

        // cleanup previous
        if (hlsRef.current) {
            try { hlsRef.current.destroy(); } catch (_) {}
            hlsRef.current = null;
        }

        (async () => {
            const url = await findWorkingHlsUrl(MEDIA_MTX_HOST, camera.id);
            if (!mounted) return;
            if (!url) {
                console.warn(`No HLS playlist found for ${camera.id} (focus)`);
                return;
            }

            if (Hls.isSupported()) {
                const hlsInstance = new Hls();
                hlsRef.current = hlsInstance;

                const onAttached = () => {
                    if (!mounted) return;
                    try { hlsInstance.loadSource(url); } catch (_) {}
                };

                const onManifest = () => {
                    if (!mounted) return;
                    videoElement.play().catch((err: any) => {
                        if (err && err.name !== 'AbortError') console.error("Focus video play failed:", err);
                    });
                };

                hlsInstance.on(Hls.Events.MEDIA_ATTACHED, onAttached);
                hlsInstance.on(Hls.Events.MANIFEST_PARSED, onManifest);
                hlsInstance.attachMedia(videoElement);

            } else if (videoElement.canPlayType('application/vnd.apple.mpegurl')) {
                videoElement.src = url;
                videoElement.play().catch((err: any) => {
                    if (err && err.name !== 'AbortError') console.error("Focus video play failed:", err);
                });
            }
        })();

        return () => {
            mounted = false;
            if (hlsRef.current) {
                try { (hlsRef.current as any).stopLoad && (hlsRef.current as any).stopLoad(); } catch (_) {}
                try { hlsRef.current.destroy(); } catch (_) {}
                hlsRef.current = null;
            }
            try { videoElement.pause(); } catch (_) {}
        };
    }, [camera.id]);

    return (
        <div className="fixed inset-0 bg-black/80 backdrop-blur-sm flex items-center justify-center z-50">
            <div className="relative bg-slate-900 rounded-xl shadow-2xl w-full max-w-4xl border border-slate-700">
                 <div className="p-4 flex justify-between items-center border-b border-slate-800">
                    <div>
                        <h3 className="text-xl font-bold text-white">{camera.specificLocation}</h3>
                        <p className="text-sm text-slate-400">{`${camera.ward}, ${camera.district}, ${camera.city}`}</p>
                    </div>
                    <button onClick={onClose} className="p-2 rounded-full hover:bg-slate-800">
                        <X size={20} />
                    </button>
                 </div>
                 <div className="aspect-video"> {/* Đảm bảo tỷ lệ khung hình */}
                     <video 
                         ref={videoRef}
                         autoPlay
                         muted={false} // Có thể không cần muted khi ở chế độ focus
                         loop
                         controls // Thêm controls để người dùng tương tác
                         className="w-full h-full object-cover"
                         poster={`https://picsum.photos/seed/${camera.id}/1280/720`} 
                     />
                 </div>
                 <div className="p-4 bg-slate-900/50 rounded-b-xl flex justify-between items-center">
                     <span className={`px-3 py-1 text-sm font-medium rounded-full border ${getStatusBadgeColor(camera.status)}`}>
                         {camera.status}
                     </span>
                     <span className="text-sm text-slate-400 font-mono">{new Date().toLocaleString()}</span>
                 </div>
            </div>
        </div>
    );
};

// --- Component Chính ---
const LiveStreams: React.FC = () => {
    const [cameras] = useState<Camera[]>(MOCK_CAMERAS);
    const [focusedCamera, setFocusedCamera] = useState<Camera | null>(null);

    return (
        <div>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6">
                {cameras.map((camera) => (
                    <CameraCard key={camera.id} camera={camera} onFocus={setFocusedCamera} />
                ))}
            </div>
            {focusedCamera && <FocusModal camera={focusedCamera} onClose={() => setFocusedCamera(null)} />}
        </div>
    );
};

export default LiveStreams;
