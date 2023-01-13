use std::fmt::Display;

use js_sys::Array;
use js_sys::ArrayBuffer;
use js_sys::Uint8Array;
use serde::de::Deserialize;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use worker_sys::console_log;
use worker_sys::d1::D1Database as D1DatabaseSys;
use worker_sys::d1::D1ExecResult;
use worker_sys::d1::D1PreparedStatement as D1PreparedStatementSys;
use worker_sys::d1::D1Result as D1ResultSys;

use crate::env::EnvBinding;
use crate::Error;
use crate::Result;

pub struct D1Database(D1DatabaseSys);

impl D1Database {
    pub fn prepare(&self, query: &str) -> D1PreparedStatement {
        self.0.prepare(query).into()
    }

    pub async fn dump(&self) -> Result<Vec<u8>> {
        let array_buffer = JsFuture::from(self.0.dump()).await?;
        let array_buffer = array_buffer.dyn_into::<ArrayBuffer>()?;
        let array = Uint8Array::new(&array_buffer);
        let mut vec = Vec::with_capacity(array.length() as usize);
        array.copy_to(&mut vec);
        Ok(vec)
    }

    pub async fn batch(&self, statements: Vec<D1PreparedStatement>) -> Result<Vec<D1Result>> {
        let statements = statements.into_iter().map(|s| s.0).collect::<Array>();
        let results = JsFuture::from(self.0.batch(statements)).await?;
        let results = results.dyn_into::<Array>()?;
        let mut vec = Vec::with_capacity(results.length() as usize);
        for result in results.iter() {
            let result = result.dyn_into::<D1ResultSys>()?;
            vec.push(D1Result(result));
        }
        Ok(vec)
    }

    pub async fn exec(&self, query: &str) -> Result<D1ExecResult> {
        let result = JsFuture::from(self.0.exec(query)).await?;
        Ok(result.into())
    }
}

impl EnvBinding for D1Database {
    const TYPE_NAME: &'static str = "BetaDatabase";
}

impl JsCast for D1Database {
    fn instanceof(val: &JsValue) -> bool {
        val.is_instance_of::<D1DatabaseSys>()
    }

    fn unchecked_from_js(val: JsValue) -> Self {
        Self(val.into())
    }

    fn unchecked_from_js_ref(val: &JsValue) -> &Self {
        unsafe { &*(val as *const JsValue as *const Self) }
    }
}

impl From<D1Database> for JsValue {
    fn from(database: D1Database) -> Self {
        JsValue::from(database.0)
    }
}

impl AsRef<JsValue> for D1Database {
    fn as_ref(&self) -> &JsValue {
        &self.0
    }
}

impl From<D1DatabaseSys> for D1Database {
    fn from(inner: D1DatabaseSys) -> Self {
        Self(inner)
    }
}

pub struct D1PreparedStatement(D1PreparedStatementSys);

impl D1PreparedStatement {
    pub fn bind<T>(&self, value: &T) -> Result<D1PreparedStatement>
    where
        T: serde::ser::Serialize + ?Sized + Display,
    {
        let value = match serde_wasm_bindgen::to_value(value) {
            Ok(v) => v,
            Err(err) => {
                return Err(Error::Internal(JsValue::from_str(
                    format!("Error converting param {} to JsValue - {}", value, err).as_str(),
                )))
            }
        };
        let array = Array::of1(&value);
        let stmt = match self.0.bind(array) {
            Ok(v) => v,
            Err(jsv) => {
                return Err(Error::BindingError(format!(
                    "Error binding to statement - {:?}",
                    jsv
                )))
            }
        };
        Ok(D1PreparedStatement(stmt))
    }

    pub async fn first<T>(&self, col_name: Option<&str>) -> Result<T>
    where
        T: for<'a> Deserialize<'a>,
    {
        let js_value = JsFuture::from(self.0.first(col_name)).await?;
        let value = serde_wasm_bindgen::from_value(js_value)?;
        Ok(value)
    }

    pub async fn run(&self) -> Result<D1Result> {
        let result = JsFuture::from(self.0.run()).await?;
        Ok(D1Result(result.into()))
    }

    pub async fn all(&self) -> Result<D1Result> {
        let promise = self.0.all();
        let result = JsFuture::from(promise).await;
        console_log!("{:?}", self.0);
        let jsv = match result {
            Ok(f) => f,
            Err(err) => return Err(Error::JsError(err)),
        };
        Ok(D1Result(jsv.into()))
    }

    pub async fn raw<T>(&self) -> Result<Vec<T>>
    where
        T: for<'a> Deserialize<'a>,
    {
        let result = JsFuture::from(self.0.raw()).await?;
        let result = result.dyn_into::<Array>()?;
        let mut vec = Vec::with_capacity(result.length() as usize);
        for value in result.iter() {
            let value = serde_wasm_bindgen::from_value(value)?;
            vec.push(value);
        }
        Ok(vec)
    }
}

impl From<D1PreparedStatementSys> for D1PreparedStatement {
    fn from(inner: D1PreparedStatementSys) -> Self {
        Self(inner)
    }
}

pub struct D1Result(D1ResultSys);

impl D1Result {
    pub fn success(&self) -> bool {
        self.0.success()
    }

    pub fn error(&self) -> Option<String> {
        self.0.error()
    }

    pub fn results<T>(&self) -> Result<Vec<T>>
    where
        T: for<'a> Deserialize<'a>,
    {
        if let Some(results) = self.0.results() {
            let mut vec = Vec::with_capacity(results.length() as usize);
            for result in results.iter() {
                let result = serde_wasm_bindgen::from_value(result)?;
                vec.push(result);
            }
            Ok(vec)
        } else {
            Ok(Vec::new())
        }
    }
}
