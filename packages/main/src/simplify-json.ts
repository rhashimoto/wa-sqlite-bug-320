import buffer from "@turf/buffer";
import convex from "@turf/convex";

export const SPATIAL_SIMPLIFY_UDF_NAME = "convert_to_simple_polygon";
const BUFFER_UNITS = 3; // 3 meters. Sqlite encodes as 32-bit float, so can only get a resolution of about 2.5m

export const simplifyFeature = (geometryJson: string): string | null => {
  if (!geometryJson) {
    return null;
  }

  try {
    const geometry: GeoJSON.Geometry = JSON.parse(geometryJson);
    const feature: GeoJSON.Feature = {
      type: "Feature",
      geometry,
      properties: {},
    };

    let simplifiedGeometry:
      | GeoJSON.Polygon
      | GeoJSON.MultiPolygon
      | null
      | undefined;
    const geomType = feature.geometry.type;

    switch (geomType) {
      case "Point":
        const bufferedPoint = buffer(feature, BUFFER_UNITS, {
          units: "meters",
        });
        simplifiedGeometry = bufferedPoint?.geometry;
        break;

      case "LineString":
        const bufferedLine = buffer(feature, BUFFER_UNITS, { units: "meters" });
        simplifiedGeometry = bufferedLine?.geometry;
        break;

      case "Polygon":
        if (feature.geometry.coordinates.length > 1) {
          simplifiedGeometry = {
            type: "Polygon",
            coordinates: [feature.geometry.coordinates[0]],
          };
        } else {
          simplifiedGeometry = feature.geometry as GeoJSON.Polygon;
        }
        break;

      case "MultiPoint":
      case "MultiLineString":
      case "MultiPolygon":
        const simplePolygon = convex(feature);
        simplifiedGeometry = simplePolygon?.geometry;
        break;

      default:
        return null;
    }

    if (
      !simplifiedGeometry ||
      !simplifiedGeometry.coordinates ||
      simplifiedGeometry.coordinates[0].length < 4
    ) {
      return null;
    }

    const firstCoord = simplifiedGeometry.coordinates[0][0];
    const lastCoord =
      simplifiedGeometry.coordinates[0][
        simplifiedGeometry.coordinates[0].length - 1
      ];
    if (firstCoord[0] !== lastCoord[0] || firstCoord[1] !== lastCoord[1]) {
      return null;
    }

    return JSON.stringify(simplifiedGeometry.coordinates[0]);
  } catch (error) {
    console.error("Error in simplifyFeature:", error);
    return null;
  }
};
