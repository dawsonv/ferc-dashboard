export const BA_REGIONS: Record<string, string[]> = {
  "All Regions": ["All Regions"],
  "California (CISO)": ["CISO", "BANC", "IID", "LDWP", "TIDC", "WALC"],
  "Northwest (NW)": ["BPAT", "AVA", "CHPD", "DOPD", "GCPD", "GRID", "IPCO", "NWMT", "PACE", "PACW", "PGE", "PSEI", "SCL", "TPWR", "WAUW"],
  "Southwest (SW)": ["AZPS", "EPE", "NEVP", "PNM", "SRP", "TEPC", "WALC", "WACM"],
  "Midwest (MISO)": ["MISO", "ALTE", "ALTW", "AMIL", "AMMO", "AMRN", "BREC", "CIN", "CONS", "DECO", "DPC", "GRE", "HE", "IPL", "MEC", "MECS", "MP", "NSP", "OTP", "SIGE", "SMP", "UPPC", "WEC", "WPS"],
  "Central (SPP)": ["SPP", "AECI", "EDE", "GRDA", "INDN", "KCPL", "LES", "NPPD", "OKGE", "OPPD", "SPA", "SPS", "SWPP", "WAUE", "WFEC", "WR"],
  "Southeast (SE)": ["SOCO", "AEC", "CPLE", "CPLW", "DUK", "FPC", "FPL", "GVL", "HST", "JEA", "LGEE", "SC", "SCEG", "TAL", "TEC", "TVA", "YAD"],
  "Mid-Atlantic (PJM)": ["PJM", "AEBN", "AEP", "AP", "BC", "CE", "DAY", "DE", "DL", "DOM", "DPL", "DUQ", "EKPC", "FE", "JC", "ME", "PE", "PEP", "PL", "PN", "PS", "RE"],
  "New York (NYISO)": ["NYIS"],
  "New England (ISONE)": ["ISNE"],
  "Canada / Other": ["AESO", "BCHA", "CFE", "HQT", "IESO", "MHEB", "NBSO", "SPC"]
};

export const ALL_BAS_FLAT = Array.from(new Set(
  Object.values(BA_REGIONS).flat().filter(ba => ba !== "All Regions")
)).sort();

export const S3_PATH = "s3://pudl.catalyst.coop/ferceqr/core_ferceqr__contracts/*.parquet";
