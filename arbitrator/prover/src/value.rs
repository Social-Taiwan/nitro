// Copyright 2021-2022, Offchain Labs, Inc.
// For license information, see https://github.com/nitro/blob/master/LICENSE

use crate::{binary::FloatType, utils::Bytes32};
use arbutil::Color;
use wasmparser::{FuncType, Type};

use digest::Digest;
use eyre::{bail, Result};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, TryFromInto};
use sha3::Keccak256;
use std::{
    convert::{TryFrom, TryInto},
    fmt::Display,
};

#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum ArbValueType {
    I32,
    I64,
    F32,
    F64,
    RefNull,
    FuncRef,
    InternalRef,
}

impl ArbValueType {
    pub fn serialize(self) -> u8 {
        self as u8
    }
}

impl TryFrom<Type> for ArbValueType {
    type Error = eyre::Error;

    fn try_from(ty: Type) -> Result<ArbValueType> {
        use Type::*;
        Ok(match ty {
            I32 => Self::I32,
            I64 => Self::I64,
            F32 => Self::F32,
            F64 => Self::F64,
            FuncRef => Self::FuncRef,
            ExternRef => Self::FuncRef,
            V128 => bail!("128-bit types are not supported"),

            // TODO: removed in wasmparser 0.95+
            ExnRef => bail!("Type not used in newer versions of wasmparser"),
            Func => bail!("Type not used in newer versions of wasmparser"),
            EmptyBlockType => bail!("Type not used in newer versions of wasmparser"),
        })
    }
}

#[cfg(feature = "native")]
pub fn parser_type(ty: &wasmer::Type) -> wasmer::wasmparser::Type {
    match ty {
        wasmer::Type::I32 => wasmer::wasmparser::Type::I32,
        wasmer::Type::I64 => wasmer::wasmparser::Type::I64,
        wasmer::Type::F32 => wasmer::wasmparser::Type::F32,
        wasmer::Type::F64 => wasmer::wasmparser::Type::F64,
        wasmer::Type::V128 => wasmer::wasmparser::Type::V128,
        wasmer::Type::ExternRef => wasmer::wasmparser::Type::ExternRef,
        wasmer::Type::FuncRef => wasmer::wasmparser::Type::FuncRef,
    }
}

#[cfg(feature = "native")]
pub fn parser_func_type(ty: wasmer::FunctionType) -> FuncType {
    let convert = |t: &[wasmer::Type]| -> Vec<Type> { t.iter().map(parser_type).collect() };
    let params = convert(ty.params()).into_boxed_slice();
    let returns = convert(ty.results()).into_boxed_slice();
    FuncType { params, returns }
}

impl From<FloatType> for ArbValueType {
    fn from(ty: FloatType) -> ArbValueType {
        match ty {
            FloatType::F32 => ArbValueType::F32,
            FloatType::F64 => ArbValueType::F64,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash, Serialize, Deserialize)]
pub enum IntegerValType {
    I32,
    I64,
}

impl From<IntegerValType> for ArbValueType {
    fn from(ty: IntegerValType) -> ArbValueType {
        match ty {
            IntegerValType::I32 => ArbValueType::I32,
            IntegerValType::I64 => ArbValueType::I64,
        }
    }
}

#[serde_as]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgramCounter {
    #[serde_as(as = "TryFromInto<usize>")]
    pub module: u32,
    #[serde_as(as = "TryFromInto<usize>")]
    pub func: u32,
    #[serde_as(as = "TryFromInto<usize>")]
    pub inst: u32,
}

#[cfg(not(any(
    target_pointer_width = "32",
    target_pointer_width = "64",
    target_pointer_width = "128"
)))]
compile_error!("Architectures with less than a 32 bit pointer width are not supported");

impl ProgramCounter {
    pub fn serialize(self) -> Bytes32 {
        let mut b = [0u8; 32];
        b[28..].copy_from_slice(&(self.inst as u32).to_be_bytes());
        b[24..28].copy_from_slice(&(self.func as u32).to_be_bytes());
        b[20..24].copy_from_slice(&(self.module as u32).to_be_bytes());
        Bytes32(b)
    }

    // These casts are safe because we checked above that a usize is at least as big as a u32

    pub fn module(self) -> usize {
        self.module as usize
    }

    pub fn func(self) -> usize {
        self.func as usize
    }

    pub fn inst(self) -> usize {
        self.inst as usize
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum Value {
    I32(u32),
    I64(u64),
    F32(f32),
    F64(f64),
    RefNull,
    FuncRef(u32),
    InternalRef(ProgramCounter),
}

impl Value {
    pub fn ty(self) -> ArbValueType {
        match self {
            Value::I32(_) => ArbValueType::I32,
            Value::I64(_) => ArbValueType::I64,
            Value::F32(_) => ArbValueType::F32,
            Value::F64(_) => ArbValueType::F64,
            Value::RefNull => ArbValueType::RefNull,
            Value::FuncRef(_) => ArbValueType::FuncRef,
            Value::InternalRef(_) => ArbValueType::InternalRef,
        }
    }

    pub fn contents_for_proof(self) -> Bytes32 {
        match self {
            Value::I32(x) => x.into(),
            Value::I64(x) => x.into(),
            Value::F32(x) => x.to_bits().into(),
            Value::F64(x) => x.to_bits().into(),
            Value::RefNull => Bytes32::default(),
            Value::FuncRef(x) => x.into(),
            Value::InternalRef(pc) => pc.serialize(),
        }
    }

    pub fn serialize_for_proof(self) -> [u8; 33] {
        let mut ret = [0u8; 33];
        ret[0] = self.ty().serialize();
        ret[1..].copy_from_slice(&*self.contents_for_proof());
        ret
    }

    pub fn is_i32_zero(self) -> bool {
        match self {
            Value::I32(0) => true,
            Value::I32(_) => false,
            _ => panic!(
                "WASM validation failed: i32.eqz equivalent called on {:?}",
                self,
            ),
        }
    }

    pub fn is_i64_zero(self) -> bool {
        match self {
            Value::I64(0) => true,
            Value::I64(_) => false,
            _ => panic!(
                "WASM validation failed: i64.eqz equivalent called on {:?}",
                self,
            ),
        }
    }

    pub fn assume_u32(self) -> u32 {
        match self {
            Value::I32(x) => x,
            _ => panic!("WASM validation failed: assume_u32 called on {:?}", self),
        }
    }

    pub fn assume_u64(self) -> u64 {
        match self {
            Value::I64(x) => x,
            _ => panic!("WASM validation failed: assume_u64 called on {:?}", self),
        }
    }

    pub fn hash(self) -> Bytes32 {
        let mut h = Keccak256::new();
        h.update(b"Value:");
        h.update([self.ty() as u8]);
        h.update(self.contents_for_proof());
        h.finalize().into()
    }

    pub fn default_of_type(ty: ArbValueType) -> Value {
        match ty {
            ArbValueType::I32 => Value::I32(0),
            ArbValueType::I64 => Value::I64(0),
            ArbValueType::F32 => Value::F32(0.),
            ArbValueType::F64 => Value::F64(0.),
            ArbValueType::RefNull | ArbValueType::FuncRef | ArbValueType::InternalRef => {
                Value::RefNull
            }
        }
    }

    pub fn pretty_print(&self) -> String {
        let lparem = "(".grey();
        let rparem = ")".grey();

        macro_rules! single {
            ($ty:expr, $value:expr) => {{
                format!("{}{}{}{}", $ty.grey(), lparem, $value, rparem)
            }};
        }
        macro_rules! pair {
            ($ty:expr, $left:expr, $right:expr) => {{
                let eq = "=".grey();
                format!(
                    "{}{}{} {} {}{}",
                    $ty.grey(),
                    lparem,
                    $left,
                    eq,
                    $right,
                    rparem
                )
            }};
        }
        match self {
            Value::I32(value) => {
                if (*value as i32) < 0 {
                    pair!("i32", *value as i32, value)
                } else {
                    single!("i32", *value)
                }
            }
            Value::I64(value) => {
                if (*value as i64) < 0 {
                    pair!("i64", *value as i64, value)
                } else {
                    single!("i64", *value)
                }
            }
            Value::F32(value) => single!("f32", *value),
            Value::F64(value) => single!("f64", *value),
            Value::RefNull => "null".into(),
            Value::FuncRef(func) => format!("func {}", func),
            Value::InternalRef(pc) => format!("inst {} in {}-{}", pc.inst, pc.module, pc.func),
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let text = self.pretty_print();
        write!(f, "{}", text)
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        self.ty() == other.ty() && self.contents_for_proof() == other.contents_for_proof()
    }
}

impl From<u32> for Value {
    fn from(value: u32) -> Self {
        Value::I32(value as u32)
    }
}

impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Value::I64(value as u64)
    }
}

impl From<f32> for Value {
    fn from(value: f32) -> Self {
        Value::F32(value)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Value::F64(value)
    }
}

impl TryInto<u32> for Value {
    type Error = ();

    fn try_into(self) -> Result<u32, Self::Error> {
        match self {
            Value::I32(value) => Ok(value as u32),
            _ => Err(()),
        }
    }
}

impl TryInto<u64> for Value {
    type Error = ();

    fn try_into(self) -> Result<u64, Self::Error> {
        match self {
            Value::I64(value) => Ok(value as u64),
            _ => Err(()),
        }
    }
}

impl Eq for Value {}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionType {
    pub inputs: Vec<ArbValueType>,
    pub outputs: Vec<ArbValueType>,
}

impl FunctionType {
    pub fn new(inputs: Vec<ArbValueType>, outputs: Vec<ArbValueType>) -> FunctionType {
        FunctionType { inputs, outputs }
    }

    pub fn hash(&self) -> Bytes32 {
        let mut h = Keccak256::new();
        h.update(b"Function type:");
        h.update(Bytes32::from(self.inputs.len()));
        for input in &self.inputs {
            h.update([*input as u8]);
        }
        h.update(Bytes32::from(self.outputs.len()));
        for output in &self.outputs {
            h.update([*output as u8]);
        }
        h.finalize().into()
    }
}

impl TryFrom<FuncType> for FunctionType {
    type Error = eyre::Error;

    fn try_from(func: FuncType) -> Result<Self> {
        let mut inputs = vec![];
        let mut outputs = vec![];

        for input in func.params.iter() {
            inputs.push(ArbValueType::try_from(*input)?)
        }
        for output in func.returns.iter() {
            outputs.push(ArbValueType::try_from(*output)?)
        }
        Ok(Self { inputs, outputs })
    }
}

impl Display for FunctionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut signature = "λ(".to_string();
        if !self.inputs.is_empty() {
            for arg in &self.inputs {
                signature += &format!("{}, ", arg);
            }
            signature.pop();
            signature.pop();
        }
        signature += ")";

        let output_tuple = self.outputs.len() > 2;
        if !self.outputs.is_empty() {
            signature += " -> ";
            if output_tuple {
                signature += "(";
            }
            for out in &self.outputs {
                signature += &format!("{}, ", out);
            }
            signature.pop();
            signature.pop();
            if output_tuple {
                signature += ")";
            }
        }
        write!(f, "{}", signature)
    }
}

impl Display for ArbValueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use ArbValueType::*;
        match self {
            I32 => write!(f, "i32"),
            I64 => write!(f, "i64"),
            F32 => write!(f, "f32"),
            F64 => write!(f, "f64"),
            RefNull => write!(f, "null"),
            FuncRef => write!(f, "func"),
            InternalRef => write!(f, "internal"),
        }
    }
}
