use pru_core::postings::decode_sorted_u64;
use pru_core::segment::SegmentReader;
use pyo3::prelude::*;
use pyo3::types::PyList;

#[pyclass]
pub struct PRUReader {
    seg_path: String,
    reader: Option<SegmentReader>,
}

#[pymethods]
impl PRUReader {
    #[new]
    pub fn new(seg_path: String) -> PyResult<Self> {
        Ok(Self {
            seg_path,
            reader: None,
        })
    }

    pub fn resolve(&mut self, py: Python<'_>, key: &[u8]) -> PyResult<PyObject> {
        if self.reader.is_none() {
            self.reader = Some(
                SegmentReader::open(&self.seg_path)
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?,
            );
        }
        let out = if let Some(v) = self.reader.as_ref().unwrap().get(key) {
            decode_sorted_u64(v)
        } else {
            Vec::new()
        };
        Ok(PyList::new(py, out).into_py(py))
    }
}

#[pymodule]
fn pru_py(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PRUReader>()?;
    Ok(())
}
