export const MOCK_CAMERAS = [
  {
    id: "cam_01",
    name: "Entrance Gate",
    location: "Main Gate",
    enabled: true,
    rtspUrl: "rtsp://localhost:8554/cam_01",
  },
  {
    id: "cam_02",
    name: "Parking Lot A",
    location: "North Lot",
    enabled: true,
    rtspUrl: "rtsp://localhost:8554/cam_02",
  },
  {
    id: "cam_03",
    name: "Parking Lot B",
    location: "South Lot",
    enabled: true,
    rtspUrl: "rtsp://localhost:8554/cam_03",
  },
  {
    id: "cam_04",
    name: "Building Entrance",
    location: "Front Door",
    enabled: true,
    rtspUrl: "rtsp://localhost:8554/cam_04",
  },
  {
    id: "cam_05",
    name: "Corridor Level 1",
    location: "1st Floor",
    enabled: true,
    rtspUrl: "rtsp://localhost:8554/cam_05",
  },
  {
    id: "cam_06",
    name: "Corridor Level 2",
    location: "2nd Floor",
    enabled: true,
    rtspUrl: "rtsp://localhost:8554/cam_06",
  },
  {
    id: "cam_07",
    name: "Common Area 1",
    location: "Lobby",
    enabled: true,
    rtspUrl: "rtsp://localhost:8554/cam_07",
  },
  {
    id: "cam_08",
    name: "Common Area 2",
    location: "Cafeteria",
    enabled: true,
    rtspUrl: "rtsp://localhost:8554/cam_08",
  },
  {
    id: "cam_09",
    name: "Stairwell A",
    location: "East Stairs",
    enabled: true,
    rtspUrl: "rtsp://localhost:8554/cam_09",
  },
  {
    id: "cam_10",
    name: "Stairwell B",
    location: "West Stairs",
    enabled: true,
    rtspUrl: "rtsp://localhost:8554/cam_10",
  },
  {
    id: "cam_11",
    name: "Loading Dock",
    location: "Rear Area",
    enabled: true,
    rtspUrl: "rtsp://localhost:8554/cam_11",
  },
  {
    id: "cam_12",
    name: "Rooftop",
    location: "Building Top",
    enabled: true,
    rtspUrl: "rtsp://localhost:8554/cam_12",
  },
  {
    id: "cam_13",
    name: "Outdoor Path",
    location: "Garden",
    enabled: true,
    rtspUrl: "rtsp://localhost:8554/cam_13",
  },
  {
    id: "cam_14",
    name: "Perimeter Fence",
    location: "North Gate",
    enabled: true,
    rtspUrl: "rtsp://localhost:8554/cam_14",
  },
  {
    id: "cam_15",
    name: "Exit Route",
    location: "Emergency Exit",
    enabled: true,
    rtspUrl: "rtsp://localhost:8554/cam_15",
  },
];

// Route HLS through Vite proxy /hls/* → MediaMTX port 8888
export const findHlsUrl = (cameraId) => {
  return `/hls/${cameraId}/index.m3u8`;
};
