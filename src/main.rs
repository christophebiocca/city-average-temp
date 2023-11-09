use clap::Parser;
use dialoguer;
use itertools::{iproduct, Itertools};
use netcdf3::{self, FileReader};
use reqwest;
use serde::{self, Deserialize, Serialize};
use std::fmt::{self, Display};
use std::fs::File;
use std::iter::Sum;
use std::ops::Div;
use std::path::{Path, PathBuf};
use std::str::FromStr;

/// Half degree resolution cells.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LonLatCell {
    half_degrees_lon_start: i16,
    half_degrees_lat_start: i16,
}

impl LonLatCell {
    fn containing(lon: f32, lat: f32) -> Self {
        Self {
            half_degrees_lon_start: (lon / 0.5).floor() as i16,
            half_degrees_lat_start: (lat / 0.5).floor() as i16,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Time {
    days_since_jan_1_1900: u32,
}

impl Time {
    fn new(days_since_jan_1_1900: f32) -> Self {
        Self {
            days_since_jan_1_1900: days_since_jan_1_1900 as u32,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct Temperature {
    celsius: f32,
}

#[derive(Debug)]
struct MissingData(Time);

impl Temperature {
    fn new(celsius: f32) -> Self {
        Self { celsius }
    }

    fn average(
        datapoints: impl Iterator<Item = (Time, Option<Self>)>,
    ) -> Result<Self, MissingData> {
        let temperatures = datapoints
            .map(|(time, temp)| temp.ok_or(MissingData(time)))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(temperatures.iter().copied().sum::<Temperature>() / temperatures.len())
    }
}

impl Sum for Temperature {
    fn sum<I>(iter: I) -> Self
    where
        I: Iterator<Item = Self>,
    {
        Self {
            celsius: iter.map(|t| t.celsius).sum(),
        }
    }
}

impl Div<usize> for Temperature {
    type Output = Self;

    fn div(self, rhs: usize) -> Self::Output {
        Self {
            celsius: self.celsius / (rhs as f32),
        }
    }
}

impl Display for Temperature {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        self.celsius.fmt(fmt)
    }
}

#[derive(Debug)]
struct TemperatureDataset {
    observations: Vec<(LonLatCell, Time, Option<Temperature>)>,
}

#[derive(Debug)]
enum TemperatureDatasetReadErr {
    CantReadFile(netcdf3::ReadError),
    UnexpectedDimensions(Vec<String>),
    TemperatureVariableMissing,
    CantReadVariable(&'static str, netcdf3::ReadError),
    MissingMissingValueAttribute,
}

impl TemperatureDataset {
    fn new(path: &Path) -> Result<Self, TemperatureDatasetReadErr> {
        let mut reader = FileReader::open(path).map_err(TemperatureDatasetReadErr::CantReadFile)?;
        let temp = reader
            .data_set()
            .get_var("tmp")
            .ok_or(TemperatureDatasetReadErr::TemperatureVariableMissing)?;
        if temp.dim_names() != &["time", "lat", "lon"] {
            return Err(TemperatureDatasetReadErr::UnexpectedDimensions(
                temp.dim_names(),
            ));
        }
        let temp_missing = temp
            .get_attr_f32("missing_value")
            .ok_or(TemperatureDatasetReadErr::MissingMissingValueAttribute)?[0];

        let observations = reader
            .read_var_f32("tmp")
            .map_err(|e| TemperatureDatasetReadErr::CantReadVariable("tmp", e))?
            .into_iter()
            .zip_eq(iproduct!(
                reader
                    .read_var_f32("time")
                    .map_err(|e| TemperatureDatasetReadErr::CantReadVariable("time", e))?,
                reader
                    .read_var_f32("lat")
                    .map_err(|e| TemperatureDatasetReadErr::CantReadVariable("lat", e))?,
                reader
                    .read_var_f32("lon")
                    .map_err(|e| TemperatureDatasetReadErr::CantReadVariable("lon", e))?
            ))
            .map(|(tmp, (time, lat, lon))| {
                if tmp == temp_missing {
                    (LonLatCell::containing(lon, lat), Time::new(time), None)
                } else {
                    (
                        LonLatCell::containing(lon, lat),
                        Time::new(time),
                        Some(Temperature::new(tmp)),
                    )
                }
            })
            .collect();

        Ok(Self { observations })
    }

    fn temperature_series_at(
        &self,
        geo: LonLatCell,
    ) -> impl Iterator<Item = (Time, Option<Temperature>)> + '_ {
        self.observations
            .iter()
            .filter(move |&&(c, _, _)| c == geo)
            .map(|&(_, time, temp)| (time, temp))
    }

    fn average_temperature_at(&self, geo: LonLatCell) -> Result<Temperature, MissingData> {
        Temperature::average(self.temperature_series_at(geo))
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
struct City {
    city: String,
    country: String,
    wikidata_entity_id: Option<String>,
    wikidata_longitude: Option<f32>,
    wikidata_latitude: Option<f32>,
    average_temperature: Option<f32>,
}

fn find_wikidata_entity_id(city: &str, country: &str) -> Result<String, reqwest::Error> {
    let client = reqwest::blocking::Client::new();

    #[derive(Deserialize, Debug)]
    struct SearchResponse {
        search: Vec<SearchResult>,
    }

    #[derive(Deserialize, Debug)]
    struct SearchResult {
        id: String,
        label: String,
        description: Option<String>,
    }

    let mut search_string = city.to_string();

    loop {
        let mut resp: SearchResponse = client
            .get("https://www.wikidata.org/w/api.php?")
            .header("Accept", "application/json")
            .header("User-Agent", "Christophe's geolocator helper script.")
            .query(&[
                ("action", "wbsearchentities"),
                ("search", &search_string),
                ("type", "item"),
                ("format", "json"),
                ("language", "en"),
            ])
            .send()?
            .json()?;

        let mut options: Vec<String> = resp
            .search
            .iter()
            .map(|result| {
                format!(
                    "{}: {}",
                    result.label,
                    result
                        .description
                        .as_ref()
                        .map(|s| &s[..])
                        .unwrap_or("No Description")
                )
            })
            .collect();
        options.push("None of these are right, change the search string".to_string());

        let choice = dialoguer::Select::new()
            .with_prompt(format!("Select match for {}, {}", city, country))
            .items(&options)
            .interact()
            .expect("User didn't make a choice.");

        if choice < resp.search.len() {
            return Ok(resp.search.remove(choice).id);
        } else {
            search_string = dialoguer::Input::new()
                .with_prompt(format!("Edit search string for {}, {}", city, country))
                .with_initial_text(format!("{} {}", city, country))
                .interact_text()
                .expect("User didn't enter a new search string.")
        }
    }
}

fn acquire_wikidata_lon_lat(wikidata_entity_id: &str) -> Result<(f32, f32), reqwest::Error> {
    let client = reqwest::blocking::Client::new();

    #[derive(Deserialize)]
    struct WikidataResponse {
        results: WikidataResults,
    }

    #[derive(Deserialize)]
    struct WikidataResults {
        bindings: Vec<WikidataEntry>,
    }

    #[derive(Deserialize)]
    struct WikidataEntry {
        lon: WikidataDouble,
        lat: WikidataDouble,
    }

    #[derive(Deserialize)]
    struct WikidataDouble {
        #[serde(deserialize_with = "parse_float")]
        value: f32,
    }

    fn parse_float<'de, D>(deserializer: D) -> Result<f32, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        f32::from_str(&s).map_err(serde::de::Error::custom)
    }

    let query = format!(
        "SELECT ?lon ?lat WHERE {{ \
              wd:{} p:P625 [
                psv:P625 [
                  wikibase:geoLongitude ?lon;
                  wikibase:geoLatitude  ?lat;
                ]
              ].
          }}",
        wikidata_entity_id
    );

    let resp: WikidataResponse = client
        .get("https://query.wikidata.org/sparql")
        .header("Accept", "application/sparql-results+json")
        .header("User-Agent", "Christophe's geolocator helper script.")
        .query(&[("query", query.trim())])
        .send()?
        .json()?;

    assert!(resp.results.bindings.len() >= 1);
    let entry = &resp.results.bindings[0];
    Ok((entry.lon.value, entry.lat.value))
}

impl City {
    fn fill_or_update_geo_information(&mut self) -> Result<LonLatCell, reqwest::Error> {
        let entity_id = match self.wikidata_entity_id {
            Some(ref entity_id) => entity_id,
            None => {
                let id = find_wikidata_entity_id(&self.city, &self.country)?;
                self.wikidata_entity_id.insert(id)
            }
        };
        let (lon, lat) = acquire_wikidata_lon_lat(&entity_id)?;
        self.wikidata_longitude = Some(lon);
        self.wikidata_latitude = Some(lat);

        Ok(LonLatCell::containing(lon, lat))
    }
}

#[derive(Parser)]
#[command(author, version, about)]
struct Args {
    /// Which path to read the temperature dataset from.
    /// We expect a NetCDF file from https://crudata.uea.ac.uk/cru/data/hrg/ with a temperature variable.
    temperature_dataset: PathBuf,
    /// Which path to read the list of cities from.
    /// CSV format, with city and country name fields.
    /// Will also allow pre-filling of the wikidata fields,
    /// and will take those as a given.
    cities: PathBuf,
    /// Where to write the output.
    output: PathBuf,
}

fn main() {
    let args = Args::parse();

    let cities_file = File::open(args.cities).expect("Couldn't open cities file");
    let mut cities_reader = csv::Reader::from_reader(cities_file);

    let mut cities = cities_reader
        .deserialize()
        .collect::<Result<Vec<City>, _>>()
        .expect("Couldn't read city data from input.");

    let dataset =
        TemperatureDataset::new(&args.temperature_dataset).expect("Couldn't read temperature data");

    for city_index in 0..(cities.len()) {
        {
            let city = &mut cities[city_index];
            let geo_cell = city
                .fill_or_update_geo_information()
                .expect("Couldn't fill in geo information.");
            city.average_temperature = Some(
                dataset
                    .average_temperature_at(geo_cell)
                    .expect("Couldn't find average temperature")
                    .celsius,
            );
        }

        let output_file = File::create(&args.output).expect("Couldn't open output file");
        let mut output_writer = csv::Writer::from_writer(output_file);
        for city in cities.iter() {
            output_writer
                .serialize(city)
                .expect("Couldn't write city out to output file");
        }
    }
}
