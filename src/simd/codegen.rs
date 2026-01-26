//! Code Generation for SIMD Operations
//!
//! This module provides code generation for SIMD operations,
//! producing bytecode or machine code for vectorized execution.

use super::{SimdLevel, VectorDataType, VectorOp};
use crate::error::{BlazeError, Result};

/// Generated code representation.
#[derive(Debug, Clone)]
pub struct GeneratedCode {
    /// Bytecode representation.
    pub bytecode: Vec<u8>,
    /// Human-readable assembly.
    pub assembly: String,
    /// Instructions.
    pub instructions: Vec<Instruction>,
    /// Register allocation.
    pub registers: Vec<Register>,
    /// Estimated cycles.
    pub estimated_cycles: usize,
}

impl GeneratedCode {
    /// Create new generated code.
    pub fn new() -> Self {
        Self {
            bytecode: Vec::new(),
            assembly: String::new(),
            instructions: Vec::new(),
            registers: Vec::new(),
            estimated_cycles: 0,
        }
    }

    /// Add an instruction.
    pub fn add_instruction(&mut self, inst: Instruction) {
        self.instructions.push(inst);
    }

    /// Finalize the code generation.
    pub fn finalize(&mut self) {
        // Generate bytecode from instructions
        for inst in &self.instructions {
            self.bytecode.extend(inst.encode());
        }

        // Generate assembly representation
        self.assembly = self
            .instructions
            .iter()
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join("\n");

        // Estimate cycles
        self.estimated_cycles = self.instructions.iter().map(|i| i.latency()).sum();
    }
}

impl Default for GeneratedCode {
    fn default() -> Self {
        Self::new()
    }
}

/// Virtual register for code generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Register {
    /// Register ID.
    pub id: u8,
    /// Register type.
    pub reg_type: RegisterType,
    /// Width in bits.
    pub width: u16,
}

impl Register {
    /// Create a new register.
    pub fn new(id: u8, reg_type: RegisterType, width: u16) -> Self {
        Self {
            id,
            reg_type,
            width,
        }
    }

    /// Create a general purpose register.
    pub fn gpr(id: u8) -> Self {
        Self::new(id, RegisterType::General, 64)
    }

    /// Create a SIMD register.
    pub fn simd(id: u8, width: u16) -> Self {
        Self::new(id, RegisterType::Simd, width)
    }

    /// Create a mask register.
    pub fn mask(id: u8) -> Self {
        Self::new(id, RegisterType::Mask, 64)
    }
}

/// Register type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegisterType {
    /// General purpose register.
    General,
    /// SIMD register.
    Simd,
    /// Mask register.
    Mask,
}

/// Instruction in the generated code.
#[derive(Debug, Clone)]
pub struct Instruction {
    /// Operation code.
    pub opcode: OpCode,
    /// Destination register.
    pub dest: Option<Register>,
    /// Source registers.
    pub sources: Vec<Register>,
    /// Immediate value.
    pub immediate: Option<i64>,
    /// Memory operand.
    pub memory: Option<MemoryOperand>,
}

impl Instruction {
    /// Create a new instruction.
    pub fn new(opcode: OpCode) -> Self {
        Self {
            opcode,
            dest: None,
            sources: Vec::new(),
            immediate: None,
            memory: None,
        }
    }

    /// Set destination register.
    pub fn with_dest(mut self, reg: Register) -> Self {
        self.dest = Some(reg);
        self
    }

    /// Add source register.
    pub fn with_source(mut self, reg: Register) -> Self {
        self.sources.push(reg);
        self
    }

    /// Set immediate value.
    pub fn with_immediate(mut self, imm: i64) -> Self {
        self.immediate = Some(imm);
        self
    }

    /// Set memory operand.
    pub fn with_memory(mut self, mem: MemoryOperand) -> Self {
        self.memory = Some(mem);
        self
    }

    /// Encode instruction to bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Encode opcode
        bytes.push(self.opcode as u8);

        // Encode destination
        if let Some(dest) = &self.dest {
            bytes.push(dest.id);
        }

        // Encode sources
        bytes.push(self.sources.len() as u8);
        for src in &self.sources {
            bytes.push(src.id);
        }

        // Encode immediate
        if let Some(imm) = self.immediate {
            bytes.extend(&imm.to_le_bytes());
        }

        bytes
    }

    /// Get instruction latency.
    pub fn latency(&self) -> usize {
        match self.opcode {
            OpCode::Load | OpCode::Store => 4,
            OpCode::Add | OpCode::Sub => 1,
            OpCode::Mul => 3,
            OpCode::Div => 15,
            OpCode::And | OpCode::Or | OpCode::Xor => 1,
            OpCode::Cmp => 1,
            OpCode::Shuffle => 3,
            OpCode::Broadcast => 1,
            OpCode::Gather => 8,
            OpCode::Scatter => 10,
            OpCode::Blend => 2,
            OpCode::Reduce => 5,
            _ => 1,
        }
    }
}

impl std::fmt::Display for Instruction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut parts = vec![format!("{:?}", self.opcode).to_lowercase()];

        if let Some(dest) = &self.dest {
            parts.push(format!("r{}", dest.id));
        }

        for src in &self.sources {
            parts.push(format!("r{}", src.id));
        }

        if let Some(imm) = self.immediate {
            parts.push(format!("${}", imm));
        }

        if let Some(mem) = &self.memory {
            parts.push(format!("[r{} + {}]", mem.base.id, mem.offset));
        }

        write!(f, "{}", parts.join(", "))
    }
}

/// Operation code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OpCode {
    /// No operation.
    Nop = 0,
    /// Load from memory.
    Load = 1,
    /// Store to memory.
    Store = 2,
    /// Move between registers.
    Move = 3,
    /// Add.
    Add = 10,
    /// Subtract.
    Sub = 11,
    /// Multiply.
    Mul = 12,
    /// Divide.
    Div = 13,
    /// Bitwise AND.
    And = 20,
    /// Bitwise OR.
    Or = 21,
    /// Bitwise XOR.
    Xor = 22,
    /// Compare.
    Cmp = 30,
    /// Compare greater than.
    CmpGt = 31,
    /// Compare less than.
    CmpLt = 32,
    /// Compare equal.
    CmpEq = 33,
    /// Shuffle elements.
    Shuffle = 40,
    /// Broadcast scalar.
    Broadcast = 41,
    /// Gather from memory.
    Gather = 42,
    /// Scatter to memory.
    Scatter = 43,
    /// Blend with mask.
    Blend = 44,
    /// Horizontal reduce.
    Reduce = 50,
    /// Minimum.
    Min = 51,
    /// Maximum.
    Max = 52,
    /// Jump.
    Jump = 60,
    /// Conditional jump.
    JumpIf = 61,
    /// Return.
    Return = 70,
}

/// Memory operand.
#[derive(Debug, Clone)]
pub struct MemoryOperand {
    /// Base register.
    pub base: Register,
    /// Offset.
    pub offset: i32,
    /// Scale factor.
    pub scale: u8,
    /// Index register.
    pub index: Option<Register>,
}

impl MemoryOperand {
    /// Create a new memory operand.
    pub fn new(base: Register, offset: i32) -> Self {
        Self {
            base,
            offset,
            scale: 1,
            index: None,
        }
    }

    /// With scale factor.
    pub fn with_scale(mut self, scale: u8) -> Self {
        self.scale = scale;
        self
    }

    /// With index register.
    pub fn with_index(mut self, index: Register) -> Self {
        self.index = Some(index);
        self
    }
}

/// Code generator for SIMD operations.
pub struct CodeGenerator {
    /// Target SIMD level.
    simd_level: SimdLevel,
    /// Next register ID.
    next_reg: u8,
}

impl CodeGenerator {
    /// Create a new code generator.
    pub fn new(simd_level: SimdLevel) -> Self {
        Self {
            simd_level,
            next_reg: 0,
        }
    }

    /// Allocate a new register.
    fn alloc_reg(&mut self, reg_type: RegisterType, width: u16) -> Register {
        let reg = Register::new(self.next_reg, reg_type, width);
        self.next_reg += 1;
        reg
    }

    /// Reset register allocation.
    fn reset_regs(&mut self) {
        self.next_reg = 0;
    }

    /// Get vector width for the target.
    fn vector_width(&self) -> u16 {
        (self.simd_level.vector_width() * 8) as u16
    }

    /// Generate code for a vector operation.
    pub fn generate_op(
        &mut self,
        op: VectorOp,
        _data_type: VectorDataType,
    ) -> Result<GeneratedCode> {
        self.reset_regs();
        let mut code = GeneratedCode::new();

        let width = self.vector_width();
        let input1 = self.alloc_reg(RegisterType::Simd, width);
        let input2 = self.alloc_reg(RegisterType::Simd, width);
        let output = self.alloc_reg(RegisterType::Simd, width);

        // Load input registers
        let base_reg = self.alloc_reg(RegisterType::General, 64);
        code.add_instruction(
            Instruction::new(OpCode::Load)
                .with_dest(input1)
                .with_memory(MemoryOperand::new(base_reg, 0)),
        );

        code.add_instruction(
            Instruction::new(OpCode::Load)
                .with_dest(input2)
                .with_memory(MemoryOperand::new(
                    base_reg,
                    self.simd_level.vector_width() as i32,
                )),
        );

        // Generate operation
        let opcode = match op {
            VectorOp::Add => OpCode::Add,
            VectorOp::Sub => OpCode::Sub,
            VectorOp::Mul => OpCode::Mul,
            VectorOp::Div => OpCode::Div,
            VectorOp::And => OpCode::And,
            VectorOp::Or => OpCode::Or,
            VectorOp::Xor => OpCode::Xor,
            VectorOp::Min => OpCode::Min,
            VectorOp::Max => OpCode::Max,
            _ => return Err(BlazeError::not_implemented(format!("Op {:?}", op))),
        };

        code.add_instruction(
            Instruction::new(opcode)
                .with_dest(output)
                .with_source(input1)
                .with_source(input2),
        );

        // Store result
        code.add_instruction(
            Instruction::new(OpCode::Store)
                .with_source(output)
                .with_memory(MemoryOperand::new(base_reg, 0)),
        );

        code.registers = vec![input1, input2, output, base_reg];
        code.finalize();

        Ok(code)
    }

    /// Generate code for a filter operation.
    pub fn generate_filter(
        &mut self,
        comparison: VectorOp,
        _data_type: VectorDataType,
    ) -> Result<GeneratedCode> {
        self.reset_regs();
        let mut code = GeneratedCode::new();

        let width = self.vector_width();
        let input = self.alloc_reg(RegisterType::Simd, width);
        let threshold = self.alloc_reg(RegisterType::Simd, width);
        let mask = self.alloc_reg(RegisterType::Mask, 64);

        let base_reg = self.alloc_reg(RegisterType::General, 64);

        // Load input
        code.add_instruction(
            Instruction::new(OpCode::Load)
                .with_dest(input)
                .with_memory(MemoryOperand::new(base_reg, 0)),
        );

        // Broadcast threshold
        code.add_instruction(
            Instruction::new(OpCode::Broadcast)
                .with_dest(threshold)
                .with_immediate(0), // Placeholder for actual threshold
        );

        // Compare
        let cmp_opcode = match comparison {
            VectorOp::Gt => OpCode::CmpGt,
            VectorOp::Lt => OpCode::CmpLt,
            VectorOp::Eq => OpCode::CmpEq,
            _ => OpCode::Cmp,
        };

        code.add_instruction(
            Instruction::new(cmp_opcode)
                .with_dest(mask)
                .with_source(input)
                .with_source(threshold),
        );

        // Store mask
        code.add_instruction(
            Instruction::new(OpCode::Store)
                .with_source(mask)
                .with_memory(MemoryOperand::new(
                    base_reg,
                    self.simd_level.vector_width() as i32,
                )),
        );

        code.registers = vec![input, threshold, mask, base_reg];
        code.finalize();

        Ok(code)
    }

    /// Generate code for an aggregate operation.
    pub fn generate_aggregate(
        &mut self,
        agg_op: VectorOp,
        _data_type: VectorDataType,
    ) -> Result<GeneratedCode> {
        self.reset_regs();
        let mut code = GeneratedCode::new();

        let width = self.vector_width();
        let input = self.alloc_reg(RegisterType::Simd, width);
        let accumulator = self.alloc_reg(RegisterType::Simd, width);
        let result = self.alloc_reg(RegisterType::General, 64);

        let base_reg = self.alloc_reg(RegisterType::General, 64);
        let counter = self.alloc_reg(RegisterType::General, 64);

        // Initialize accumulator based on operation
        let init_val = match agg_op {
            VectorOp::Sum | VectorOp::Count => 0,
            VectorOp::Min => i64::MAX,
            VectorOp::Max => i64::MIN,
            _ => 0,
        };

        code.add_instruction(
            Instruction::new(OpCode::Broadcast)
                .with_dest(accumulator)
                .with_immediate(init_val),
        );

        // Load input chunk
        code.add_instruction(
            Instruction::new(OpCode::Load)
                .with_dest(input)
                .with_memory(MemoryOperand::new(base_reg, 0)),
        );

        // Reduce operation
        let reduce_op = match agg_op {
            VectorOp::Sum => OpCode::Add,
            VectorOp::Min => OpCode::Min,
            VectorOp::Max => OpCode::Max,
            _ => OpCode::Add,
        };

        code.add_instruction(
            Instruction::new(reduce_op)
                .with_dest(accumulator)
                .with_source(accumulator)
                .with_source(input),
        );

        // Horizontal reduce
        code.add_instruction(
            Instruction::new(OpCode::Reduce)
                .with_dest(result)
                .with_source(accumulator),
        );

        // Store result
        code.add_instruction(
            Instruction::new(OpCode::Store)
                .with_source(result)
                .with_memory(MemoryOperand::new(base_reg, 0)),
        );

        code.registers = vec![input, accumulator, result, base_reg, counter];
        code.finalize();

        Ok(code)
    }

    /// Generate code for fused operations.
    pub fn generate_fused(
        &mut self,
        ops: &[VectorOp],
        _data_type: VectorDataType,
    ) -> Result<GeneratedCode> {
        self.reset_regs();
        let mut code = GeneratedCode::new();

        if ops.is_empty() {
            return Err(BlazeError::invalid_argument("No operations to fuse"));
        }

        let width = self.vector_width();
        let input1 = self.alloc_reg(RegisterType::Simd, width);
        let input2 = self.alloc_reg(RegisterType::Simd, width);
        let mut current = self.alloc_reg(RegisterType::Simd, width);

        let base_reg = self.alloc_reg(RegisterType::General, 64);

        // Load inputs
        code.add_instruction(
            Instruction::new(OpCode::Load)
                .with_dest(input1)
                .with_memory(MemoryOperand::new(base_reg, 0)),
        );

        code.add_instruction(
            Instruction::new(OpCode::Load)
                .with_dest(input2)
                .with_memory(MemoryOperand::new(base_reg, width as i32 / 8)),
        );

        // First operation
        let first_op = self.map_vector_op(ops[0])?;
        code.add_instruction(
            Instruction::new(first_op)
                .with_dest(current)
                .with_source(input1)
                .with_source(input2),
        );

        // Subsequent operations
        for &op in &ops[1..] {
            let next = self.alloc_reg(RegisterType::Simd, width);
            let opcode = self.map_vector_op(op)?;

            code.add_instruction(
                Instruction::new(opcode)
                    .with_dest(next)
                    .with_source(current)
                    .with_source(input2),
            );

            current = next;
        }

        // Store final result
        code.add_instruction(
            Instruction::new(OpCode::Store)
                .with_source(current)
                .with_memory(MemoryOperand::new(base_reg, 0)),
        );

        code.finalize();
        Ok(code)
    }

    /// Map VectorOp to OpCode.
    fn map_vector_op(&self, op: VectorOp) -> Result<OpCode> {
        match op {
            VectorOp::Add => Ok(OpCode::Add),
            VectorOp::Sub => Ok(OpCode::Sub),
            VectorOp::Mul => Ok(OpCode::Mul),
            VectorOp::Div => Ok(OpCode::Div),
            VectorOp::And => Ok(OpCode::And),
            VectorOp::Or => Ok(OpCode::Or),
            VectorOp::Xor => Ok(OpCode::Xor),
            VectorOp::Min => Ok(OpCode::Min),
            VectorOp::Max => Ok(OpCode::Max),
            VectorOp::Eq => Ok(OpCode::CmpEq),
            VectorOp::Lt => Ok(OpCode::CmpLt),
            VectorOp::Gt => Ok(OpCode::CmpGt),
            _ => Err(BlazeError::not_implemented(format!("Op {:?}", op))),
        }
    }
}

impl Default for CodeGenerator {
    fn default() -> Self {
        Self::new(SimdLevel::best_available())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generated_code() {
        let mut code = GeneratedCode::new();
        code.add_instruction(Instruction::new(OpCode::Add));
        code.finalize();

        assert!(!code.bytecode.is_empty());
        assert!(!code.assembly.is_empty());
    }

    #[test]
    fn test_register_creation() {
        let gpr = Register::gpr(0);
        assert_eq!(gpr.reg_type, RegisterType::General);
        assert_eq!(gpr.width, 64);

        let simd = Register::simd(1, 256);
        assert_eq!(simd.reg_type, RegisterType::Simd);
        assert_eq!(simd.width, 256);
    }

    #[test]
    fn test_instruction_encoding() {
        let inst = Instruction::new(OpCode::Add)
            .with_dest(Register::simd(0, 256))
            .with_source(Register::simd(1, 256))
            .with_source(Register::simd(2, 256));

        let encoded = inst.encode();
        assert!(!encoded.is_empty());
        assert_eq!(encoded[0], OpCode::Add as u8);
    }

    #[test]
    fn test_instruction_display() {
        let inst = Instruction::new(OpCode::Add)
            .with_dest(Register::gpr(0))
            .with_source(Register::gpr(1))
            .with_source(Register::gpr(2));

        let display = inst.to_string();
        assert!(display.contains("add"));
        assert!(display.contains("r0"));
    }

    #[test]
    fn test_code_generator_add() {
        let mut gen = CodeGenerator::new(SimdLevel::Avx2);
        let code = gen.generate_op(VectorOp::Add, VectorDataType::I32).unwrap();

        assert!(!code.instructions.is_empty());
        assert!(!code.bytecode.is_empty());
    }

    #[test]
    fn test_code_generator_filter() {
        let mut gen = CodeGenerator::new(SimdLevel::Avx2);
        let code = gen
            .generate_filter(VectorOp::Gt, VectorDataType::I32)
            .unwrap();

        assert!(!code.instructions.is_empty());
        // Should have compare instruction
        assert!(code.instructions.iter().any(|i| i.opcode == OpCode::CmpGt));
    }

    #[test]
    fn test_code_generator_aggregate() {
        let mut gen = CodeGenerator::new(SimdLevel::Avx2);
        let code = gen
            .generate_aggregate(VectorOp::Sum, VectorDataType::I64)
            .unwrap();

        assert!(!code.instructions.is_empty());
        // Should have reduce instruction
        assert!(code.instructions.iter().any(|i| i.opcode == OpCode::Reduce));
    }

    #[test]
    fn test_code_generator_fused() {
        let mut gen = CodeGenerator::new(SimdLevel::Avx2);
        let ops = vec![VectorOp::Add, VectorOp::Mul];
        let code = gen.generate_fused(&ops, VectorDataType::I32).unwrap();

        assert!(!code.instructions.is_empty());
        // Should have both Add and Mul
        assert!(code.instructions.iter().any(|i| i.opcode == OpCode::Add));
        assert!(code.instructions.iter().any(|i| i.opcode == OpCode::Mul));
    }

    #[test]
    fn test_memory_operand() {
        let reg = Register::gpr(0);
        let mem = MemoryOperand::new(reg, 16)
            .with_scale(4)
            .with_index(Register::gpr(1));

        assert_eq!(mem.offset, 16);
        assert_eq!(mem.scale, 4);
        assert!(mem.index.is_some());
    }

    #[test]
    fn test_instruction_latency() {
        assert_eq!(Instruction::new(OpCode::Add).latency(), 1);
        assert_eq!(Instruction::new(OpCode::Mul).latency(), 3);
        assert_eq!(Instruction::new(OpCode::Div).latency(), 15);
        assert_eq!(Instruction::new(OpCode::Load).latency(), 4);
    }

    #[test]
    fn test_estimated_cycles() {
        let mut code = GeneratedCode::new();
        code.add_instruction(Instruction::new(OpCode::Load)); // 4 cycles
        code.add_instruction(Instruction::new(OpCode::Add)); // 1 cycle
        code.add_instruction(Instruction::new(OpCode::Store)); // 4 cycles
        code.finalize();

        assert_eq!(code.estimated_cycles, 9);
    }
}
